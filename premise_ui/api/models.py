"""Request models for the local Premise UI API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ProjectPathRequest(BaseModel):
    path: str


class ProjectSaveRequest(BaseModel):
    path: str
    project: dict = Field(default_factory=dict)


class ProjectCloneRequest(BaseModel):
    source_path: str
    target_path: str


class DialogFilter(BaseModel):
    label: str
    pattern: str


class PathDialogRequest(BaseModel):
    mode: Literal["open_file", "save_file", "open_directory"]
    title: str | None = None
    initial_path: str | None = None
    default_extension: str | None = None
    must_exist: bool = True
    filters: list[DialogFilter] = Field(default_factory=list)


class RecentRememberRequest(BaseModel):
    kind: Literal[
        "project",
        "source_directory",
        "export_directory",
        "scenario_file",
        "other",
    ]
    path: str
    label: str | None = None
    base_path: str | None = None


class RunDiagnosticsRequest(BaseModel):
    run_id: str | None = None
    project_path: str | None = None
    run_dir: str | None = None


class ProjectRunRequest(BaseModel):
    path: str | None = None
    project: dict = Field(default_factory=dict)
    dry_run: bool = True


class ScenarioPreviewRequest(BaseModel):
    path: str


class BrightwayProjectRequest(BaseModel):
    project_name: str


class ScenarioExplorerSummaryRequest(BaseModel):
    scenario_paths: list[str] = Field(default_factory=list)
    sector: str
    group_names: list[str] = Field(default_factory=list)
    regions: list[str] = Field(default_factory=list)
    variables: list[str] = Field(default_factory=list)
    year_start: int | None = None
    year_end: int | None = None


class ScenarioExplorerCompareRequest(BaseModel):
    scenario_paths: list[str] = Field(default_factory=list)
    sector: str
    compare_mode: Literal["overlay", "indexed", "delta", "percent_change"] = "overlay"
    baseline_year: int | None = None
    baseline_scenario_id: str | None = None
    group_names: list[str] = Field(default_factory=list)
    regions: list[str] = Field(default_factory=list)
    variables: list[str] = Field(default_factory=list)
    year_start: int | None = None
    year_end: int | None = None


class IamKeyUpdateRequest(BaseModel):
    value: str
    remember: bool = True


class JobValidateRequest(BaseModel):
    run_manifest: dict = Field(default_factory=dict)


class JobEnqueueRequest(BaseModel):
    run_manifest: dict = Field(default_factory=dict)
    dry_run: bool = True


class JobCancelRequest(BaseModel):
    run_id: str
