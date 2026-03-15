"""Pure queue and status helpers for worker orchestration."""

from __future__ import annotations

from typing import Any, Callable


def queue_position(job_queue: list[dict[str, Any]], run_id: str) -> int | None:
    for position, job in enumerate(job_queue, start=1):
        if job["run_id"] == run_id:
            return position
    return None


def pop_queued_job(job_queue: list[dict[str, Any]], run_id: str) -> dict[str, Any] | None:
    position = queue_position(job_queue, run_id)
    if position is None:
        return None
    return job_queue.pop(position - 1)


def status_from_events(
    events: list[dict[str, Any]],
    *,
    process_returncode: int | None = None,
    is_active: bool = False,
    queue_position: int | None = None,
) -> str:
    if events:
        for event in reversed(events):
            event_type = event.get("event_type")
            if event_type == "job_completed":
                return "completed"
            if event_type == "job_failed":
                return "failed"
            if event_type == "job_cancelled":
                return "cancelled"
            if event_type == "job_started":
                return "running"

    if queue_position is not None:
        return "queued"

    if is_active:
        return "running"

    if process_returncode is not None:
        if process_returncode == 0:
            return "completed"
        if process_returncode < 0:
            return "cancelled"
        return "failed"

    return "queued"


def advance_queue(
    app_state: Any,
    *,
    sync_finished_run: Callable[[str], None],
    spawn_job: Callable[[dict[str, Any]], None],
) -> None:
    active_run_id = getattr(app_state, "active_run_id", None)
    if active_run_id:
        process = app_state.processes.get(active_run_id)
        if process is not None and process.poll() is None:
            return

        sync_finished_run(active_run_id)
        app_state.active_run_id = None

    if app_state.job_queue:
        next_job = app_state.job_queue.pop(0)
        spawn_job(next_job)


def queue_or_start_job(
    app_state: Any,
    job: dict[str, Any],
    *,
    spawn_job: Callable[[dict[str, Any]], None],
) -> tuple[str, int | None]:
    if getattr(app_state, "active_run_id", None) is None:
        spawn_job(job)
        return "running", None

    app_state.job_queue.append(job)
    return "queued", len(app_state.job_queue)
