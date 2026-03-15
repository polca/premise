import { useState } from "react";
import type { DragEvent, ReactNode } from "react";

export type ExplorerSelectionMode = "all" | "custom" | "none";

type ExplorerDragPayload = {
  source: "available" | "selected";
  value: string;
};

export function explorerSelectionValues(
  mode: ExplorerSelectionMode,
  selected: string[],
  options: string[],
): string[] {
  const valid = selected.filter((entry) => options.includes(entry));
  if (mode === "all") {
    return options;
  }
  if (mode === "none") {
    return [];
  }
  return valid;
}

export function sanitizeExplorerSelection(
  mode: ExplorerSelectionMode,
  selected: string[],
  options: string[],
): { mode: ExplorerSelectionMode; selected: string[] } {
  const valid = selected.filter((entry) => options.includes(entry));
  if (mode === "all" || mode === "none") {
    return { mode, selected: [] };
  }
  if (valid.length) {
    return { mode: "custom", selected: valid };
  }
  return options.length ? { mode: "all", selected: [] } : { mode: "none", selected: [] };
}

function reorderExplorerSelection(
  items: string[],
  draggedValue: string,
  targetValue: string,
): string[] {
  if (
    draggedValue === targetValue ||
    !items.includes(draggedValue) ||
    !items.includes(targetValue)
  ) {
    return items;
  }

  const next = items.filter((entry) => entry !== draggedValue);
  const targetIndex = next.indexOf(targetValue);
  if (targetIndex === -1) {
    return [...next, draggedValue];
  }
  next.splice(targetIndex, 0, draggedValue);
  return next;
}

function setExplorerDragPayload(
  event: DragEvent<HTMLElement>,
  payload: ExplorerDragPayload,
) {
  event.dataTransfer.effectAllowed = "move";
  event.dataTransfer.setData("text/plain", JSON.stringify(payload));
}

function getExplorerDragPayload(event: DragEvent<HTMLElement>): ExplorerDragPayload | null {
  const raw = event.dataTransfer.getData("text/plain");
  if (!raw) {
    return null;
  }

  try {
    const payload = JSON.parse(raw) as Partial<ExplorerDragPayload>;
    if (
      (payload.source === "available" || payload.source === "selected") &&
      typeof payload.value === "string" &&
      payload.value
    ) {
      return { source: payload.source, value: payload.value };
    }
  } catch {
    return null;
  }

  return null;
}

export function ExplorerSelectionBoard({
  title,
  emptyMessage,
  options,
  selectionMode,
  selectedValues,
  onSetAll,
  onSetNone,
  onSetSelectedValues,
}: {
  title: ReactNode;
  emptyMessage: string;
  options: string[];
  selectionMode: ExplorerSelectionMode;
  selectedValues: string[];
  onSetAll: () => void;
  onSetNone: () => void;
  onSetSelectedValues: (values: string[]) => void;
}) {
  const [query, setQuery] = useState("");
  const normalizedQuery = query.trim().toLowerCase();
  const allSelectedValues = explorerSelectionValues(selectionMode, selectedValues, options);
  const availableValues = options.filter((entry) => !allSelectedValues.includes(entry));
  const visibleSelectedValues = normalizedQuery
    ? allSelectedValues.filter((entry) => entry.toLowerCase().includes(normalizedQuery))
    : allSelectedValues;
  const visibleAvailableValues = normalizedQuery
    ? availableValues.filter((entry) => entry.toLowerCase().includes(normalizedQuery))
    : availableValues;

  function handleAdd(value: string) {
    if (allSelectedValues.includes(value)) {
      return;
    }
    onSetSelectedValues([...allSelectedValues, value]);
  }

  function handleRemove(value: string) {
    if (!allSelectedValues.includes(value)) {
      return;
    }
    const next = allSelectedValues.filter((entry) => entry !== value);
    if (!next.length) {
      onSetNone();
      return;
    }
    onSetSelectedValues(next);
  }

  function handleSelectedDrop(event: DragEvent<HTMLElement>, targetValue?: string) {
    event.preventDefault();
    const payload = getExplorerDragPayload(event);
    if (!payload) {
      return;
    }

    if (payload.source === "available") {
      handleAdd(payload.value);
      return;
    }

    if (!allSelectedValues.includes(payload.value)) {
      return;
    }

    if (!targetValue) {
      const withoutDragged = allSelectedValues.filter((entry) => entry !== payload.value);
      onSetSelectedValues([...withoutDragged, payload.value]);
      return;
    }

    onSetSelectedValues(
      reorderExplorerSelection(allSelectedValues, payload.value, targetValue),
    );
  }

  function handleAvailableDrop(event: DragEvent<HTMLElement>) {
    event.preventDefault();
    const payload = getExplorerDragPayload(event);
    if (!payload || payload.source !== "selected") {
      return;
    }
    handleRemove(payload.value);
  }

  return (
    <div className="explorer-selection-board">
      <div className="explorer-selection-board-head">
        <div>
          <div className="explorer-selection-title">{title}</div>
          <div className="subtle">
            {selectionMode === "all"
              ? `All ${options.length} selected`
              : selectionMode === "none"
                ? "No selections active"
                : `${allSelectedValues.length} of ${options.length} selected`}
          </div>
        </div>
        <div className="action-cluster compact-actions">
          <button className="button subtle-button" type="button" onClick={() => onSetAll()}>
            Select all
          </button>
          <button className="button subtle-button" type="button" onClick={() => onSetNone()}>
            Deselect all
          </button>
        </div>
      </div>

      {!options.length ? (
        <div className="empty-state compact-empty">{emptyMessage}</div>
      ) : (
        <>
          <input
            className="explorer-selection-search"
            type="search"
            value={query}
            placeholder="Filter available and selected items"
            onChange={(event) => setQuery(event.target.value)}
          />

          <div className="explorer-selection-columns">
            <div
              className="explorer-selection-column"
              onDragOver={(event) => event.preventDefault()}
              onDrop={handleAvailableDrop}
            >
              <div className="explorer-selection-column-head">
                <span>Available</span>
                <span className="subtle">{visibleAvailableValues.length}</span>
              </div>
              <div className="explorer-selection-list">
                {visibleAvailableValues.length ? (
                  visibleAvailableValues.map((entry) => (
                    <button
                      className="explorer-token"
                      draggable
                      key={`available-${entry}`}
                      type="button"
                      onClick={() => handleAdd(entry)}
                      onDragStart={(event) =>
                        setExplorerDragPayload(event, {
                          source: "available",
                          value: entry,
                        })
                      }
                    >
                      <span>{entry}</span>
                      <span className="explorer-token-action">+</span>
                    </button>
                  ))
                ) : (
                  <div className="explorer-selection-empty">No matching available items.</div>
                )}
              </div>
            </div>

            <div
              className="explorer-selection-column"
              onDragOver={(event) => event.preventDefault()}
              onDrop={(event) => handleSelectedDrop(event)}
            >
              <div className="explorer-selection-column-head">
                <span>Selected</span>
                <span className="subtle">{visibleSelectedValues.length}</span>
              </div>
              <div className="explorer-selection-list">
                {visibleSelectedValues.length ? (
                  visibleSelectedValues.map((entry) => (
                    <button
                      className="explorer-token explorer-token-selected"
                      draggable
                      key={`selected-${entry}`}
                      type="button"
                      onClick={() => handleRemove(entry)}
                      onDragStart={(event) =>
                        setExplorerDragPayload(event, {
                          source: "selected",
                          value: entry,
                        })
                      }
                      onDragOver={(event) => event.preventDefault()}
                      onDrop={(event) => handleSelectedDrop(event, entry)}
                    >
                      <span>{entry}</span>
                      <span className="explorer-token-action">×</span>
                    </button>
                  ))
                ) : (
                  <div className="explorer-selection-empty">No selected items.</div>
                )}
              </div>
            </div>
          </div>

          <div className="inline-note">
            Click items to move them between columns, or drag within <strong>Selected</strong> to
            change plotting order.
          </div>
        </>
      )}
    </div>
  );
}
