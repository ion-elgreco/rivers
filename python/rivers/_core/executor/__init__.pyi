"""Executor strategies — how a job's steps are dispatched."""

class Executor:
    """Base class for execution strategies.

    Construct via the static factories (:meth:`in_process`, :meth:`parallel`,
    :meth:`kubernetes`) and pass to :class:`Job` or as the repository default.
    """

    class InProcess(Executor):
        """Run every step serially in the calling Python process."""

    class Parallel(Executor):
        """Run steps concurrently in subprocesses (loky pool) and async tasks."""

        max_workers: int
        """Worker subprocesses for sync steps."""
        max_async_concurrent: int | None
        """Max concurrent async tasks (``None`` = unbounded)."""

    class Kubernetes(Executor):
        """Run each step as a Kubernetes worker pod."""

        worker_image: str | None
        """Image for worker pods (defaults to the controlling pod's image)."""
        max_concurrent_steps: int | None
        """Cap on concurrently scheduled step pods (``None`` = unbounded)."""
        namespace: str | None
        """Namespace pods are launched in (``None`` = current namespace)."""
        service_account: str
        """Service account bound to worker pods."""
        worker_cpu: str
        """CPU request/limit for worker pods."""
        worker_memory: str
        """Memory request/limit for worker pods."""

    @staticmethod
    def in_process() -> Executor.InProcess:
        """Build an in-process executor."""
        ...

    @staticmethod
    def parallel(
        max_workers: int | None = None,
        max_async_concurrent: int | None = None,
    ) -> Executor.Parallel:
        """Build a multi-process executor backed by loky.

        Args:
            max_workers: Subprocess pool size (``None`` = available parallelism, respecting cgroup/CPU-affinity limits).
            max_async_concurrent: Cap on concurrent async tasks (``None`` = unbounded).
        """
        ...

    @staticmethod
    def kubernetes(
        worker_image: str | None = None,
        *,
        max_concurrent_steps: int | None = None,
        namespace: str | None = None,
        service_account: str = "rivers-executor",
        worker_cpu: str = "500m",
        worker_memory: str = "512Mi",
    ) -> Executor.Kubernetes:
        """Build a Kubernetes executor that launches one pod per step.

        Args:
            worker_image: Container image; defaults to the running image when ``None``.
            max_concurrent_steps: Cap on concurrent step pods.
            namespace: Target namespace.
            service_account: Service account to bind to step pods.
            worker_cpu: CPU request/limit for step pods.
            worker_memory: Memory request/limit for step pods.
        """
        ...

__all__ = ["Executor"]
