//! Captures a failed step's traceback (see
//! [`rivers_core::execution::traceback`]). Python's `traceback` module walks
//! the frames and the cause chain; `linecache` reads the source lines on the
//! machine that ran the step.

use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};
use rivers_core::execution::traceback::{
    CONTEXT_LINES, ChainLink, ExceptionInfo, Frame, GROUP_DEPTH, GROUP_WIDTH, Traceback,
    fold_repeats,
};

use crate::repository::resolved_node::ResolvedNode;

/// Holds a traceback captured in another process: a parallel worker captures
/// it before loky pickles the exception (the frames do not survive that), and
/// the parent sets it here when it unwraps [`crate::errors::WorkerStepError`].
pub(crate) const STASH_ATTR: &str = "_rivers_traceback";

/// `err`'s traceback as JSON for its `run_logs` row: the one a worker
/// captured, else captured here. `None` when the exception has no frames, or
/// when capture fails, so a traceback problem never hides the error itself.
pub(crate) fn capture_json(
    py: Python<'_>,
    err: &PyErr,
    app_package: Option<&str>,
) -> Option<String> {
    let exc = err.value(py);
    if let Ok(stashed) = exc.getattr(STASH_ATTR) {
        return stashed.extract().ok();
    }
    match capture(py, exc.as_any(), app_package) {
        Ok(traceback) if traceback.has_no_frames() => None,
        Ok(traceback) => Some(traceback.to_json()),
        Err(e) => {
            tracing::warn!(error = %e, "could not capture the step's traceback");
            None
        }
    }
}

/// The top-level package of the step's function. Its frames stay the user's
/// own even when the package is installed in site-packages.
pub(crate) fn app_package(func: &Bound<'_, PyAny>) -> Option<String> {
    let module: String = func.getattr("__module__").ok()?.extract().ok()?;
    let top = module.split('.').next()?;
    (!top.is_empty() && top != "__main__").then(|| top.to_string())
}

/// [`app_package`] of a plan node's function.
pub(crate) fn node_package(py: Python<'_>, node: Option<&ResolvedNode>) -> Option<String> {
    app_package(node?.callable(py).ok()?.bind(py))
}

fn capture(
    py: Python<'_>,
    exc: &Bound<'_, PyAny>,
    app_package: Option<&str>,
) -> PyResult<Traceback> {
    let te = py
        .import("traceback")?
        .getattr("TracebackException")?
        .call_method1("from_exception", (exc,))?;
    let text = PyString::new(py, "").call_method1("join", (te.call_method0("format")?,))?;
    let capture = Capture {
        paths: Paths::load(py, app_package)?,
        linecache: py.import("linecache")?.into_any(),
    };
    Ok(Traceback {
        exceptions: capture.chain(&te, exc, 0)?,
        text: lossy(&text)?,
    })
}

struct Capture<'py> {
    paths: Paths,
    linecache: Bound<'py, PyAny>,
}

impl<'py> Capture<'py> {
    /// `te` (a `TracebackException`) and the exceptions it was raised from or
    /// while handling, oldest first. `exc` is the exception `te` describes.
    fn chain(
        &self,
        te: &Bound<'py, PyAny>,
        exc: &Bound<'py, PyAny>,
        depth: usize,
    ) -> PyResult<Vec<ExceptionInfo>> {
        let mut newest_first = Vec::new();
        let mut next = Some((te.clone(), exc.clone()));
        while let Some((te, exc)) = next {
            let mut info = self.exception(&te, &exc, depth)?;
            next = linked(&te, &exc)?.map(|(link, te, exc)| {
                info.chain = Some(link);
                (te, exc)
            });
            newest_first.push(info);
        }
        newest_first.reverse();
        Ok(newest_first)
    }

    fn exception(
        &self,
        te: &Bound<'py, PyAny>,
        exc: &Bound<'py, PyAny>,
        depth: usize,
    ) -> PyResult<ExceptionInfo> {
        let ty = exc.get_type();
        let module = ty
            .module()
            .ok()
            .map(|m| m.to_string_lossy().into_owned())
            .filter(|m| m != "builtins");
        let (group, group_omitted) = self.group(te, exc, depth)?;
        Ok(ExceptionInfo {
            exc_type: ty.qualname()?.to_string_lossy().into_owned(),
            module,
            value: exception_value(exc),
            chain: None,
            frames: self.frames(&te.getattr("stack")?)?,
            group,
            group_omitted,
        })
    }

    fn group(
        &self,
        te: &Bound<'py, PyAny>,
        exc: &Bound<'py, PyAny>,
        depth: usize,
    ) -> PyResult<(Vec<Vec<ExceptionInfo>>, u32)> {
        // Python 3.10 has no exception groups.
        let Ok(members) = te.getattr("exceptions") else {
            return Ok((Vec::new(), 0));
        };
        if members.is_none() {
            return Ok((Vec::new(), 0));
        }
        let total = members.len()?;
        let shown = if depth < GROUP_DEPTH {
            total.min(GROUP_WIDTH)
        } else {
            0
        };
        let real = exc.getattr("exceptions")?;
        let group = (0..shown)
            .map(|i| self.chain(&members.get_item(i)?, &real.get_item(i)?, depth + 1))
            .collect::<PyResult<_>>()?;
        Ok((group, (total - shown) as u32))
    }

    /// `stack` (a `StackSummary`) with repeated frames folded as Python
    /// prints them.
    fn frames(&self, stack: &Bound<'py, PyAny>) -> PyResult<Vec<Frame>> {
        let summaries = stack.try_iter()?.collect::<PyResult<Vec<_>>>()?;
        let keys = summaries
            .iter()
            .map(|fs| {
                Ok((
                    lossy(&fs.getattr("filename")?)?,
                    fs.getattr("lineno")?.extract::<Option<u32>>()?,
                    lossy(&fs.getattr("name")?)?,
                ))
            })
            .collect::<PyResult<Vec<_>>>()?;
        fold_repeats(&keys)
            .into_iter()
            .map(|(i, repeated)| {
                let (abs_path, lineno, function) = keys[i].clone();
                let mut frame = self.frame(&summaries[i], abs_path, lineno, function)?;
                frame.repeated = repeated;
                Ok(frame)
            })
            .collect()
    }

    fn frame(
        &self,
        fs: &Bound<'py, PyAny>,
        abs_path: String,
        lineno: Option<u32>,
        function: String,
    ) -> PyResult<Frame> {
        let (in_app, filename) = self.paths.classify(&abs_path);
        let mut frame = Frame {
            filename,
            abs_path,
            function,
            lineno,
            colno: None,
            end_lineno: None,
            end_colno: None,
            pre_context: Vec::new(),
            context_line: None,
            post_context: Vec::new(),
            in_app,
            repeated: 0,
        };
        let Some(lineno) = lineno.filter(|&n| n > 0) else {
            return Ok(frame);
        };
        let lines = self
            .linecache
            .call_method1("getlines", (&frame.abs_path,))?;
        let lines = lines.cast::<PyList>()?;
        let line_at = |n: u32| -> Option<String> {
            let line = lines.get_item(n.checked_sub(1)? as usize).ok()?;
            Some(
                lossy(&line)
                    .ok()?
                    .trim_end_matches(['\n', '\r'])
                    .to_string(),
            )
        };
        let Some(line) = line_at(lineno) else {
            return Ok(frame);
        };
        // Python 3.11+ positions count UTF-8 bytes from 0; Sentry's columns
        // count characters from 1.
        let position = |name: &str| -> Option<u32> { fs.getattr(name).ok()?.extract().ok()? };
        if let (Some(col), Some(end_line), Some(end_col)) = (
            position("colno"),
            position("end_lineno"),
            position("end_colno"),
        ) {
            let end_text = if end_line == lineno {
                Some(line.clone())
            } else {
                line_at(end_line)
            };
            if let Some(end_text) = end_text {
                frame.colno = Some(char_offset(&line, col) + 1);
                frame.end_lineno = Some(end_line);
                frame.end_colno = Some(char_offset(&end_text, end_col));
            }
        }
        let context = if in_app { CONTEXT_LINES } else { 0 };
        frame.pre_context = (lineno.saturating_sub(context).max(1)..lineno)
            .filter_map(line_at)
            .collect();
        frame.post_context = (lineno + 1..=lineno + context).map_while(line_at).collect();
        frame.context_line = Some(line);
        Ok(frame)
    }
}

/// The exception `te` was raised from or while handling, picked as Python
/// picks it when it prints a traceback, with the matching real exception.
#[allow(clippy::type_complexity)]
fn linked<'py>(
    te: &Bound<'py, PyAny>,
    exc: &Bound<'py, PyAny>,
) -> PyResult<Option<(ChainLink, Bound<'py, PyAny>, Bound<'py, PyAny>)>> {
    let cause = te.getattr("__cause__")?;
    if !cause.is_none() {
        return Ok(Some((ChainLink::Cause, cause, exc.getattr("__cause__")?)));
    }
    let context = te.getattr("__context__")?;
    if !context.is_none() && !te.getattr("__suppress_context__")?.is_truthy()? {
        return Ok(Some((
            ChainLink::Context,
            context,
            exc.getattr("__context__")?,
        )));
    }
    Ok(None)
}

/// `str(exc)`, then its notes, one per line, as Python prints them.
fn exception_value(exc: &Bound<'_, PyAny>) -> String {
    let text = |obj: &Bound<'_, PyAny>, fallback: &str| {
        obj.str()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|_| fallback.to_string())
    };
    let mut value = text(exc, "<exception str() failed>");
    if let Ok(notes) = exc.getattr("__notes__")
        && !notes.is_instance_of::<PyString>()
        && let Ok(notes) = notes.try_iter()
    {
        for note in notes.flatten() {
            value.push('\n');
            value.push_str(&text(&note, "<note str() failed>"));
        }
    }
    value
}

fn lossy(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(obj.cast::<PyString>()?.to_string_lossy().into_owned())
}

/// Characters in the first `byte_offset` UTF-8 bytes of `line`.
fn char_offset(line: &str, byte_offset: u32) -> u32 {
    let bytes = line.as_bytes();
    let end = (byte_offset as usize).min(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).chars().count() as u32
}

/// Where library code lives, to tell it from the user's.
struct Paths {
    stdlib: Vec<String>,
    rivers: Option<String>,
    cwd: Option<String>,
    app_package: Option<String>,
}

impl Paths {
    fn load(py: Python<'_>, app_package: Option<&str>) -> PyResult<Self> {
        let paths = py.import("sysconfig")?.call_method0("get_paths")?;
        let stdlib = ["stdlib", "platstdlib"]
            .into_iter()
            .filter_map(|key| lossy(&paths.get_item(key).ok()?).ok())
            .map(|dir| dir_prefix(&dir))
            .collect();
        let rivers = py
            .import("rivers")
            .and_then(|m| m.getattr("__file__"))
            .ok()
            .and_then(|file| lossy(&file).ok())
            .and_then(|file| {
                let file = file.replace('\\', "/");
                file.rsplit_once('/').map(|(dir, _)| format!("{dir}/"))
            });
        let cwd = std::env::current_dir()
            .ok()
            .map(|dir| dir_prefix(&dir.to_string_lossy()))
            .filter(|dir| dir != "/");
        Ok(Self {
            stdlib,
            rivers,
            cwd,
            app_package: app_package.map(str::to_string),
        })
    }

    /// `(in_app, display path)` for a frame's file.
    fn classify(&self, abs_path: &str) -> (bool, String) {
        if abs_path.starts_with('<') {
            // `<frozen ...>` is Python's own; `<string>` and notebook cells
            // are the user's.
            return (!abs_path.starts_with("<frozen "), abs_path.to_string());
        }
        let path = abs_path.replace('\\', "/");
        if let Some(rel) = self
            .rivers
            .as_deref()
            .and_then(|dir| path.strip_prefix(dir))
        {
            return (false, format!("rivers/{rel}"));
        }
        if let Some(rel) = after_site_packages(&path) {
            let top = rel.split('/').next().unwrap_or_default();
            let top = top.strip_suffix(".py").unwrap_or(top);
            return (self.app_package.as_deref() == Some(top), rel.to_string());
        }
        if let Some(rel) = self
            .stdlib
            .iter()
            .find_map(|dir| path.strip_prefix(dir.as_str()))
        {
            return (false, rel.to_string());
        }
        let short = self
            .cwd
            .as_deref()
            .and_then(|dir| path.strip_prefix(dir))
            .unwrap_or(&path);
        (true, short.to_string())
    }
}

fn dir_prefix(dir: &str) -> String {
    let dir = dir.replace('\\', "/");
    if dir.ends_with('/') {
        dir
    } else {
        format!("{dir}/")
    }
}

fn after_site_packages(path: &str) -> Option<&str> {
    ["/site-packages/", "/dist-packages/"]
        .into_iter()
        .filter_map(|marker| path.rfind(marker).map(|i| &path[i + marker.len()..]))
        .next()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(app_package: Option<&str>) -> Paths {
        Paths {
            stdlib: vec!["/usr/lib/python3.13/".into()],
            rivers: Some("/src/rivers/python/rivers/".into()),
            cwd: Some("/home/me/project/".into()),
            app_package: app_package.map(str::to_string),
        }
    }

    #[test]
    fn classify_sorts_user_code_from_library_code() {
        let p = paths(None);
        assert_eq!(
            p.classify("/home/me/project/assets/sales.py"),
            (true, "assets/sales.py".into())
        );
        assert_eq!(
            p.classify("/opt/elsewhere/job.py"),
            (true, "/opt/elsewhere/job.py".into())
        );
        assert_eq!(
            p.classify("/venv/lib/python3.13/site-packages/pandas/io/common.py"),
            (false, "pandas/io/common.py".into())
        );
        assert_eq!(
            p.classify("/usr/lib/python3/dist-packages/six.py"),
            (false, "six.py".into())
        );
        assert_eq!(
            p.classify("/usr/lib/python3.13/asyncio/tasks.py"),
            (false, "asyncio/tasks.py".into())
        );
        assert_eq!(
            p.classify("/src/rivers/python/rivers/io_handlers/base.py"),
            (false, "rivers/io_handlers/base.py".into())
        );
        assert_eq!(
            p.classify("<frozen importlib._bootstrap>"),
            (false, "<frozen importlib._bootstrap>".into())
        );
        assert_eq!(p.classify("<string>"), (true, "<string>".into()));
    }

    #[test]
    fn the_step_package_is_user_code_even_in_site_packages() {
        let p = paths(Some("my_project"));
        assert_eq!(
            p.classify("/venv/lib/python3.13/site-packages/my_project/assets.py"),
            (true, "my_project/assets.py".into())
        );
        assert_eq!(
            p.classify("/venv/lib/python3.13/site-packages/my_project.py"),
            (true, "my_project.py".into())
        );
        assert!(
            !p.classify("/venv/lib/python3.13/site-packages/pandas/core.py")
                .0
        );
    }

    #[test]
    fn classify_handles_windows_paths() {
        let p = paths(None);
        assert_eq!(
            p.classify(r"C:\venv\Lib\site-packages\pandas\core.py"),
            (false, "pandas/core.py".into())
        );
    }

    #[test]
    fn char_offset_counts_characters() {
        assert_eq!(char_offset("x = d['b']", 4), 4);
        // "é" is two bytes.
        assert_eq!(char_offset("é = d['b']", 5), 4);
        assert_eq!(char_offset("short", 99), 5);
    }
}
