//! Shared helpers for browser-based component tests.
//!
//! Mounted into each `tests/<component>_browser.rs` integration test as
//! `mod common;` (the `mod.rs` placement keeps cargo from compiling this
//! as a standalone test target). Centralizes DOM setup, event dispatch,
//! and the microtask flush needed between a signal write and a follow-up
//! assertion (Leptos `Effect`s run on the next microtask).

#![cfg(target_arch = "wasm32")]
#![allow(dead_code)] // Each test file uses a different subset of helpers.

use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Document, Element, HtmlElement, MouseEvent, MouseEventInit};

pub fn document() -> Document {
    web_sys::window().unwrap().document().unwrap()
}

/// Allocate a fresh detached <div> per test so concurrent runs (and
/// successive runs in the same browser session) don't see each other's
/// nodes. Caller drops the returned `UnmountHandle` to tear down the
/// reactive subtree; the DOM node stays appended (cheap, ~bytes per test).
pub fn fresh_mount_target() -> HtmlElement {
    let doc = document();
    let div = doc.create_element("div").unwrap();
    doc.body().unwrap().append_child(&div).unwrap();
    div.dyn_into::<HtmlElement>().unwrap()
}

/// Synthesize a click on `el`, optionally with the shift modifier set.
/// Takes `&Element` (not `&HtmlElement`) so SVG nodes can also be
/// click-targeted; `dispatch_event` lives on `EventTarget`, a base of
/// both.
pub fn click(el: &Element, shift: bool) {
    let init = MouseEventInit::new();
    init.set_bubbles(true);
    init.set_cancelable(true);
    init.set_shift_key(shift);
    let ev = MouseEvent::new_with_mouse_event_init_dict("click", &init).unwrap();
    el.dispatch_event(&ev).unwrap();
}

/// Yield once to the microtask queue so any Leptos `Effect`s scheduled
/// by the preceding signal write get a chance to run before we assert.
pub async fn flush_effects() {
    let p = js_sys::Promise::resolve(&JsValue::NULL);
    let _ = JsFuture::from(p).await;
}

/// Collect every element matching `selector` under `host` as `Element`s.
/// Both inputs and outputs use `Element` (not `HtmlElement`) so callers
/// can chain queries through SVG / mount-target boundaries —
/// `query_selector(_all)` lives on `Element`, and `class_name`,
/// `text_content`, `dispatch_event`, and `get_attribute` are all on
/// `Element` or above. `HtmlElement` derefs to `Element`.
pub fn query_all<H: AsRef<Element>>(host: &H, selector: &str) -> Vec<Element> {
    let nodes = host.as_ref().query_selector_all(selector).unwrap();
    (0..nodes.length())
        .map(|i| nodes.item(i).unwrap().dyn_into::<Element>().unwrap())
        .collect()
}

/// First element matching `selector` under `host`, panicking with a
/// descriptive message when nothing matches (turns "unwrap on None" into
/// a readable test failure).
pub fn query_one<H: AsRef<Element>>(host: &H, selector: &str) -> Element {
    host.as_ref()
        .query_selector(selector)
        .unwrap()
        .unwrap_or_else(|| panic!("no element matching `{selector}`"))
}

/// Push a synthetic browser URL via `history.pushState`. The next
/// `<Router>` mount reads `window.location` at construction time, so
/// tests that need a specific `/locations/<ns>/<name>/...` path active
/// must call this first.
///
/// All tests run in the same browser session, so the URL persists
/// across tests. Each router-using test should call `nav_to(...)` with
/// its own path to avoid leakage.
pub fn nav_to(path: &str) {
    let history = web_sys::window().unwrap().history().unwrap();
    let _ = history.push_state_with_url(&JsValue::NULL, "", Some(path));
}

/// Replace `window.fetch` with a Rust handler. The handler receives the
/// resolved request URL and returns a JSON-encoded body to send back
/// with HTTP 200. Drop the returned `FetchMock` to restore the
/// original `fetch` (or rely on test cleanup — the wasm-pack runner
/// reloads the page between sessions).
///
/// Why monkey-patching: server fns under `feature = "csr"` go through
/// `gloo_net::http::Request` → `web_sys::Request` → `window.fetch`.
/// Replacing the function on the window object is the only intercept
/// point that catches every server-fn call without modifying the
/// crate's source.
pub struct FetchMock {
    _closure: wasm_bindgen::closure::Closure<dyn FnMut(JsValue, JsValue) -> js_sys::Promise>,
    original: JsValue,
}

impl Drop for FetchMock {
    fn drop(&mut self) {
        let window = web_sys::window().unwrap();
        let _ = js_sys::Reflect::set(&window, &JsValue::from_str("fetch"), &self.original);
    }
}

/// Install a `window.fetch` mock. `responder` receives the URL string
/// (resolved against `window.location`) and returns the JSON body that
/// should be sent back with `200 OK`. Returning `None` produces a 500
/// response — useful for testing error paths.
///
/// Implementation note: gloo-net (used by leptos server fns) imports
/// `fetch` as a global symbol via `#[wasm_bindgen(js_name = "fetch")]`,
/// which resolves through the scope chain at call time and therefore
/// picks up `window.fetch` overrides. The Response is built by JS
/// (`new Response(body, {status, headers})`) — synthesizing it via
/// `web_sys::Response::new_with_opt_str_and_init` produces a Response
/// that gloo-net's `Response::text()` await never resolves, so the
/// action's future hangs forever.
pub fn install_fetch_mock<F>(responder: F) -> FetchMock
where
    F: Fn(&str) -> Option<String> + 'static,
{
    use wasm_bindgen::JsCast;
    use wasm_bindgen::closure::Closure;

    let window = web_sys::window().unwrap();
    let original = js_sys::Reflect::get(&window, &JsValue::from_str("fetch")).unwrap();

    // JS factory that builds `new Response(body, {status, headers})` —
    // letting the runtime own Response construction sidesteps the
    // body-stream wiring quirks we hit with `web_sys::Response::new_*`.
    let factory_src = "(function(body, status){\
        return new Response(body, { status: status, headers: { 'content-type': 'application/json' } });\
    })";
    let factory_jsv = js_sys::eval(factory_src).expect("compile response factory");
    let factory: js_sys::Function = factory_jsv.dyn_into().unwrap();

    let closure: Closure<dyn FnMut(JsValue, JsValue) -> js_sys::Promise> =
        Closure::new(move |input: JsValue, _init: JsValue| {
            // `input` is either a string URL or a `Request` object. Pull
            // the URL out of either shape.
            let url = if let Some(s) = input.as_string() {
                s
            } else if let Ok(req) = input.dyn_into::<web_sys::Request>() {
                req.url()
            } else {
                String::new()
            };
            let body = responder(&url);
            let (body_str, status) = match body {
                Some(json) => (json, 200),
                None => (String::new(), 500),
            };
            let response_jsv = factory
                .call2(
                    &JsValue::NULL,
                    &JsValue::from_str(&body_str),
                    &JsValue::from_f64(status as f64),
                )
                .expect("response factory call");
            js_sys::Promise::resolve(&response_jsv)
        });

    let fetch_fn = closure.as_ref().unchecked_ref::<JsValue>();
    js_sys::Reflect::set(&window, &JsValue::from_str("fetch"), fetch_fn).unwrap();

    FetchMock {
        _closure: closure,
        original,
    }
}
