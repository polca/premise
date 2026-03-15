import { useEffect, useMemo, useRef, useState } from "react";
import type { MouseEvent, ReactNode } from "react";

import type { PlotSeries } from "./explorerData";

const PREVIEW_COLORS = ["#bf5c2b", "#2f7473", "#667a53", "#8e4f78"];

type PlotPoint = {
  year: number;
  value: number;
};

type ExplorerTableRow = {
  year: number;
  values: Record<string, number>;
};

function cloneSvgWithInlineStyles(svg: SVGSVGElement): SVGSVGElement {
  const clone = svg.cloneNode(true) as SVGSVGElement;
  const originalNodes = [svg, ...Array.from(svg.querySelectorAll("*"))];
  const cloneNodes = [clone, ...Array.from(clone.querySelectorAll("*"))];

  for (let index = 0; index < originalNodes.length; index += 1) {
    const originalNode = originalNodes[index];
    const cloneNode = cloneNodes[index] as Element | undefined;
    if (!cloneNode) {
      continue;
    }
    const computed = window.getComputedStyle(originalNode);
    const styleText = Array.from(computed)
      .map((property) => `${property}: ${computed.getPropertyValue(property)};`)
      .join(" ");
    cloneNode.setAttribute("style", styleText);
  }

  clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  clone.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
  return clone;
}

async function downloadPlotAsPng(frame: HTMLElement | null, filename: string) {
  if (!frame || typeof window === "undefined") {
    return;
  }

  const svg = frame.querySelector("svg.preview-svg");
  if (!(svg instanceof SVGSVGElement)) {
    throw new Error("No plot SVG was found to export.");
  }

  const serializedSvg = new XMLSerializer().serializeToString(cloneSvgWithInlineStyles(svg));
  const svgBlob = new Blob([serializedSvg], { type: "image/svg+xml;charset=utf-8" });
  const svgUrl = URL.createObjectURL(svgBlob);
  const image = new Image();

  try {
    const loadPromise = new Promise<void>((resolve, reject) => {
      image.onload = () => resolve();
      image.onerror = () => reject(new Error("Failed to prepare the plot image for export."));
    });
    image.src = svgUrl;
    await loadPromise;

    const viewBox = svg.viewBox.baseVal;
    const width = Math.max(Math.round(viewBox.width || svg.clientWidth || 1200), 1200);
    const height = Math.max(Math.round(viewBox.height || svg.clientHeight || 640), 640);
    const canvas = document.createElement("canvas");
    canvas.width = width * 2;
    canvas.height = height * 2;
    const context = canvas.getContext("2d");
    if (!context) {
      throw new Error("Canvas export is unavailable in this browser.");
    }

    context.scale(2, 2);
    context.fillStyle = "#fbf7ef";
    context.fillRect(0, 0, width, height);
    context.drawImage(image, 0, 0, width, height);

    const dataUrl = canvas.toDataURL("image/png");
    const link = document.createElement("a");
    link.href = dataUrl;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  } finally {
    URL.revokeObjectURL(svgUrl);
  }
}

function explorerTableRows(series: PlotSeries[]): ExplorerTableRow[] {
  const rowsByYear = new Map<number, ExplorerTableRow>();

  for (const entry of series) {
    for (const point of entry.points) {
      const existing =
        rowsByYear.get(point.year) ??
        {
          year: point.year,
          values: {},
        };
      existing.values[entry.label] = point.value;
      rowsByYear.set(point.year, existing);
    }
  }

  return Array.from(rowsByYear.values()).sort((left, right) => left.year - right.year);
}

export function FullscreenPlotCard({
  title,
  subtitle,
  children,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  children: ReactNode;
}) {
  const frameRef = useRef<HTMLElement | null>(null);
  const [fullscreen, setFullscreen] = useState(false);

  useEffect(() => {
    const handleFullscreenChange = () => {
      setFullscreen(document.fullscreenElement === frameRef.current);
    };

    document.addEventListener("fullscreenchange", handleFullscreenChange);
    return () => {
      document.removeEventListener("fullscreenchange", handleFullscreenChange);
    };
  }, []);

  const canFullscreen =
    typeof document !== "undefined" && document.fullscreenEnabled !== false;

  const exportFileName =
    String(title)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "") || "premise-plot";

  const toggleFullscreen = async () => {
    const frame = frameRef.current;
    if (!frame || typeof frame.requestFullscreen !== "function") {
      return;
    }

    try {
      if (document.fullscreenElement === frame) {
        await document.exitFullscreen();
      } else {
        await frame.requestFullscreen();
      }
    } catch {
      // Ignore rejected fullscreen requests. Browsers may block them in some contexts.
    }
  };

  const handleExportPng = async () => {
    try {
      await downloadPlotAsPng(frameRef.current, `${exportFileName}.png`);
    } catch (error) {
      console.error(error);
    }
  };

  return (
    <section ref={frameRef} className="explorer-plot-card explorer-plot-frame">
      <div className="explorer-plot-card-head">
        <div className="explorer-plot-title">
          <strong>{title}</strong>
          {subtitle ? <span className="subtle">{subtitle}</span> : null}
        </div>
        <div className="action-cluster compact-actions">
          <button
            className="button subtle-button icon-button"
            type="button"
            aria-label="Save plot as PNG"
            title="Save plot as PNG"
            onClick={() => {
              void handleExportPng();
            }}
          >
            ⇩
          </button>
          <button
            className="button subtle-button icon-button"
            type="button"
            aria-label={fullscreen ? "Exit full screen" : "Enter full screen"}
            title={fullscreen ? "Exit full screen" : "Enter full screen"}
            onClick={() => {
              void toggleFullscreen();
            }}
            disabled={!canFullscreen}
          >
            {fullscreen ? "⤡" : "⤢"}
          </button>
        </div>
      </div>
      {children}
    </section>
  );
}

export function SeriesPlot({
  series,
  emptyMessage,
  ariaLabel,
  chartMode = "line",
  hiddenLabels = [],
  onToggleSeries,
  onResetHiddenSeries,
  baselineYear = null,
  showZeroLine = false,
  yDomain = null,
  legendInitiallyOpen = true,
}: {
  series: PlotSeries[];
  emptyMessage: string;
  ariaLabel: string;
  chartMode?: "line" | "stacked_area" | "bar" | "stacked_bar";
  hiddenLabels?: string[];
  onToggleSeries?: (label: string) => void;
  onResetHiddenSeries?: () => void;
  baselineYear?: number | null;
  showZeroLine?: boolean;
  yDomain?: { min: number; max: number } | null;
  legendInitiallyOpen?: boolean;
}) {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoveredPoint, setHoveredPoint] = useState<{
    left: number;
    top: number;
    label: string;
    year: number;
    value: number;
    unit?: string | null;
  } | null>(null);
  const [legendOpen, setLegendOpen] = useState(legendInitiallyOpen);

  if (!series.length) {
    return <div className="empty-state">{emptyMessage}</div>;
  }

  const visibleSeries = series.filter((entry) => !hiddenLabels.includes(entry.label));
  const axisSeries = visibleSeries.length ? visibleSeries : series;
  const plottedSeries = visibleSeries;
  const allPoints = axisSeries.flatMap((entry) => entry.points);
  const years = Array.from(new Set(allPoints.map((point) => point.year))).sort(
    (left, right) => left - right,
  );
  const width = 680;
  const height = 252;
  const paddingLeft = 72;
  const paddingRight = 28;
  const paddingTop = 22;
  const paddingBottom = 32;
  const chartWidth = width - paddingLeft - paddingRight;
  const chartHeight = height - paddingTop - paddingBottom;
  const yearStart = years[0];
  const yearSpan = Math.max(1, years[years.length - 1] - yearStart);
  const canStack = plottedSeries.every((entry) =>
    entry.points.every((point) => point.value >= 0),
  );
  const effectiveChartMode =
    chartMode === "stacked_area"
      ? canStack
        ? "stacked_area"
        : "line"
      : chartMode === "stacked_bar"
        ? canStack
          ? "stacked_bar"
          : "bar"
        : chartMode;
  const seriesValueAtYear = (entry: PlotSeries, year: number) =>
    entry.points.find((point) => point.year === year)?.value ?? 0;
  const yearlyTotals =
    effectiveChartMode === "stacked_area" || effectiveChartMode === "stacked_bar"
      ? years.map((year) =>
          plottedSeries.reduce((sum, entry) => sum + seriesValueAtYear(entry, year), 0),
        )
      : [];
  const rawValues = allPoints.map((point) => point.value);
  const fallbackDomainMin =
    effectiveChartMode === "stacked_area" || effectiveChartMode === "stacked_bar"
      ? 0
      : effectiveChartMode === "bar"
        ? Math.min(0, ...rawValues)
        : Math.min(...rawValues);
  const fallbackDomainMax =
    effectiveChartMode === "stacked_area" || effectiveChartMode === "stacked_bar"
      ? Math.max(0, ...yearlyTotals)
      : effectiveChartMode === "bar"
        ? Math.max(0, ...rawValues)
        : Math.max(...rawValues);
  const domainMin = yDomain ? yDomain.min : fallbackDomainMin;
  const domainMax = yDomain ? yDomain.max : fallbackDomainMax;
  const paddedMin = domainMin === domainMax ? domainMin - 1 : domainMin;
  const paddedMax = domainMin === domainMax ? domainMax + 1 : domainMax;
  const valueSpan = Math.max(1, paddedMax - paddedMin);
  const barBaseValue = paddedMin <= 0 && paddedMax >= 0 ? 0 : paddedMax <= 0 ? paddedMax : 0;

  const x = (year: number) => paddingLeft + ((year - yearStart) / yearSpan) * chartWidth;
  const y = (value: number) =>
    height - paddingBottom - ((value - paddedMin) / valueSpan) * chartHeight;

  const setTooltip = (
    label: string,
    point: PlotPoint,
    unit: string | null | undefined,
    xValue: number,
    yValue: number,
  ) => {
    setHoveredPoint({
      left: (xValue / width) * 100,
      top: (yValue / height) * 100,
      label,
      year: point.year,
      value: point.value,
      unit,
    });
  };

  const stackedSeries =
    effectiveChartMode === "stacked_area"
      ? plottedSeries.map((entry, index) => {
          const lowerValues = years.map((year) =>
            plottedSeries
              .slice(0, index)
              .reduce((sum, current) => sum + seriesValueAtYear(current, year), 0),
          );
          const upperValues = years.map(
            (year, yearIndex) => lowerValues[yearIndex] + seriesValueAtYear(entry, year),
          );
          const topPath = years.map((year, yearIndex) => `${x(year)},${y(upperValues[yearIndex])}`);
          const bottomPath = years
            .slice()
            .reverse()
            .map((year, reverseIndex) => {
              const yearIndex = years.length - 1 - reverseIndex;
              return `${x(year)},${y(lowerValues[yearIndex])}`;
            });
          return {
            entry,
            areaPath: `M ${topPath.join(" L ")} L ${bottomPath.join(" L ")} Z`,
            upperValues,
          };
        })
      : [];

  const groupedBarWidth =
    effectiveChartMode === "bar"
      ? Math.max(
          10,
          chartWidth / Math.max(years.length, 1) / Math.max(plottedSeries.length + 1, 2),
        )
      : 0;
  const stackedBarWidth =
    effectiveChartMode === "stacked_bar"
      ? Math.max(12, chartWidth / Math.max(years.length, 1) / 1.8)
      : 0;
  const baselineMarkerVisible = baselineYear != null && years.includes(baselineYear);
  const tooltipCandidates =
    effectiveChartMode === "stacked_area"
      ? stackedSeries.flatMap(({ entry, upperValues }) =>
          years.flatMap((year, yearIndex) => {
            const point = entry.points.find((candidate) => candidate.year === year);
            if (!point) {
              return [];
            }
            return {
              label: entry.label,
              point,
              unit: entry.unit,
              xValue: x(year),
              yValue: y(upperValues[yearIndex]),
            };
          }),
        )
      : effectiveChartMode === "bar"
        ? plottedSeries.flatMap((entry, index) =>
            years.flatMap((year) => {
              const point = entry.points.find((candidate) => candidate.year === year);
              if (!point) {
                return [];
              }
              const clusterStart =
                x(year) - ((plottedSeries.length - 1) * groupedBarWidth) / 2;
              const rectX = clusterStart + index * groupedBarWidth;
              const baseY = y(barBaseValue);
              const pointY = y(point.value);
              const rectY = Math.min(pointY, baseY);
              const rectHeight = Math.abs(pointY - baseY);
              return {
                label: entry.label,
                point,
                unit: entry.unit,
                xValue: rectX + groupedBarWidth / 2,
                yValue: rectY + rectHeight / 2,
              };
            }),
          )
        : effectiveChartMode === "stacked_bar"
          ? years.flatMap((year) => {
              let lowerValue = 0;
              const rectX = x(year) - stackedBarWidth / 2;
              return plottedSeries.flatMap((entry) => {
                const point = entry.points.find((candidate) => candidate.year === year);
                if (!point) {
                  return [];
                }
                const upperValue = lowerValue + point.value;
                const rectY = y(upperValue);
                const rectHeight = Math.max(y(lowerValue) - y(upperValue), 2);
                const candidate = {
                  label: entry.label,
                  point,
                  unit: entry.unit,
                  xValue: rectX + stackedBarWidth / 2,
                  yValue: rectY + rectHeight / 2,
                };
                lowerValue = upperValue;
                return candidate;
              });
            })
          : plottedSeries.flatMap((entry) =>
              entry.points.map((point) => ({
                label: entry.label,
                point,
                unit: entry.unit,
                xValue: x(point.year),
                yValue: y(point.value),
              })),
            );

  const updateTooltipFromPointer = (event: MouseEvent<SVGSVGElement>) => {
    if (!tooltipCandidates.length || !svgRef.current) {
      setHoveredPoint(null);
      return;
    }
    const bounds = svgRef.current.getBoundingClientRect();
    if (!bounds.width || !bounds.height) {
      return;
    }
    const svgX = ((event.clientX - bounds.left) / bounds.width) * width;
    const svgY = ((event.clientY - bounds.top) / bounds.height) * height;

    let nearest = tooltipCandidates[0];
    let nearestDistance = Number.POSITIVE_INFINITY;
    for (const candidate of tooltipCandidates) {
      const dx = candidate.xValue - svgX;
      const dy = candidate.yValue - svgY;
      const distance = dx * dx + dy * dy;
      if (distance < nearestDistance) {
        nearest = candidate;
        nearestDistance = distance;
      }
    }

    setTooltip(nearest.label, nearest.point, nearest.unit, nearest.xValue, nearest.yValue);
  };

  return (
    <div className="preview-plot">
      {hoveredPoint ? (
        <div
          className="preview-tooltip"
          style={{ left: `${hoveredPoint.left}%`, top: `${hoveredPoint.top}%` }}
        >
          <strong>{hoveredPoint.label}</strong>
          <span>{hoveredPoint.year}</span>
          <span>
            {hoveredPoint.value.toLocaleString(undefined, {
              maximumFractionDigits: 3,
            })}
            {hoveredPoint.unit ? ` ${hoveredPoint.unit}` : ""}
          </span>
        </div>
      ) : null}
      <svg
        ref={svgRef}
        viewBox={`0 0 ${width} ${height}`}
        className="preview-svg"
        role="img"
        aria-label={ariaLabel}
        onMouseMove={updateTooltipFromPointer}
        onMouseLeave={() => setHoveredPoint(null)}
      >
        <rect x="0" y="0" width={width} height={height} rx="18" className="preview-bg" />
        {[0, 0.25, 0.5, 0.75, 1].map((fraction) => {
          const lineY = paddingTop + fraction * chartHeight;
          const labelValue = paddedMax - fraction * valueSpan;
          return (
            <g key={fraction}>
              <line
                x1={paddingLeft}
                y1={lineY}
                x2={width - paddingRight}
                y2={lineY}
                className="preview-grid-line"
              />
              <text
                x={paddingLeft - 14}
                y={lineY + 4}
                className="preview-axis-label"
                textAnchor="end"
              >
                {labelValue.toLocaleString(undefined, {
                  maximumFractionDigits: 1,
                })}
              </text>
            </g>
          );
        })}
        {years.map((year) => (
          <g key={year}>
            <line
              x1={x(year)}
              y1={paddingTop}
              x2={x(year)}
              y2={height - paddingBottom}
              className="preview-grid-line preview-grid-line-vertical"
            />
            <text
              x={x(year)}
              y={height - 10}
              className="preview-axis-label"
              textAnchor="middle"
            >
              {year}
            </text>
          </g>
        ))}
        {showZeroLine && paddedMin < 0 && paddedMax > 0 ? (
          <line
            x1={paddingLeft}
            y1={y(0)}
            x2={width - paddingRight}
            y2={y(0)}
            className="preview-zero-line"
          />
        ) : null}
        {baselineMarkerVisible ? (
          <line
            x1={x(baselineYear as number)}
            y1={paddingTop}
            x2={x(baselineYear as number)}
            y2={height - paddingBottom}
            className="preview-baseline-line"
          />
        ) : null}
        {effectiveChartMode === "stacked_area"
          ? stackedSeries.map(({ entry, areaPath }, index) => {
              const color = PREVIEW_COLORS[index % PREVIEW_COLORS.length];
              return (
                <g key={`${entry.label}-${index}`}>
                  <path
                    d={areaPath}
                    className="preview-area"
                    style={{ fill: color, stroke: color }}
                  />
                  {entry.points.map((point) => (
                    <circle
                      key={`${entry.label}-${point.year}`}
                      cx={x(point.year)}
                      cy={y(
                        stackedSeries
                          .slice(0, index + 1)
                          .reduce(
                            (sum, current) =>
                              sum +
                              (current.entry.points.find((candidate) => candidate.year === point.year)
                                ?.value ?? 0),
                            0,
                          ),
                      )}
                      r={3}
                      className="preview-dot"
                      style={{ fill: color }}
                      onMouseEnter={() =>
                        setTooltip(
                          entry.label,
                          point,
                          entry.unit,
                          x(point.year),
                          y(
                            stackedSeries
                              .slice(0, index + 1)
                              .reduce(
                                (sum, current) =>
                                  sum +
                                  (current.entry.points.find((candidate) => candidate.year === point.year)
                                    ?.value ?? 0),
                                0,
                              ),
                          ),
                        )
                      }
                    />
                  ))}
                </g>
              );
            })
          : null}
        {effectiveChartMode === "bar"
          ? plottedSeries.map((entry, index) => {
              const color = PREVIEW_COLORS[index % PREVIEW_COLORS.length];
              return (
                <g key={`${entry.label}-${index}`}>
                  {years.map((year, yearIndex) => {
                    const point = entry.points.find((candidate) => candidate.year === year);
                    if (!point) {
                      return null;
                    }
                    const clusterStart =
                      x(year) - ((plottedSeries.length - 1) * groupedBarWidth) / 2;
                    const rectX = clusterStart + yearIndex * 0 + index * groupedBarWidth;
                    const baseY = y(barBaseValue);
                    const pointY = y(point.value);
                    const rectY = Math.min(pointY, baseY);
                    const rectHeight = Math.abs(pointY - baseY);
                    return (
                      <rect
                        key={`${entry.label}-${year}`}
                        x={rectX}
                        y={rectY}
                        width={Math.max(groupedBarWidth - 2, 4)}
                        height={Math.max(rectHeight, 2)}
                        rx={4}
                        className="preview-bar"
                        style={{ fill: color }}
                        onMouseEnter={() =>
                          setTooltip(
                            entry.label,
                            point,
                            entry.unit,
                            rectX + groupedBarWidth / 2,
                            rectY,
                          )
                        }
                      />
                    );
                  })}
                </g>
              );
            })
          : null}
        {effectiveChartMode === "stacked_bar"
          ? years.map((year) => {
              let lowerValue = 0;
              const rectX = x(year) - stackedBarWidth / 2;
              return (
                <g key={`stacked-bar-${year}`}>
                  {plottedSeries.map((entry, index) => {
                    const point = entry.points.find((candidate) => candidate.year === year);
                    if (!point) {
                      return null;
                    }
                    const color = PREVIEW_COLORS[index % PREVIEW_COLORS.length];
                    const upperValue = lowerValue + point.value;
                    const rectY = y(upperValue);
                    const rectHeight = Math.max(y(lowerValue) - y(upperValue), 2);
                    const segment = (
                      <rect
                        key={`${entry.label}-${year}`}
                        x={rectX}
                        y={rectY}
                        width={Math.max(stackedBarWidth, 10)}
                        height={rectHeight}
                        rx={index === plottedSeries.length - 1 ? 6 : 0}
                        className="preview-bar"
                        style={{ fill: color }}
                        onMouseEnter={() =>
                          setTooltip(
                            entry.label,
                            point,
                            entry.unit,
                            rectX + stackedBarWidth / 2,
                            rectY,
                          )
                        }
                      />
                    );
                    lowerValue = upperValue;
                    return segment;
                  })}
                </g>
              );
            })
          : null}
        {effectiveChartMode === "line"
          ? plottedSeries.map((entry, index) => {
              const color = PREVIEW_COLORS[index % PREVIEW_COLORS.length];
              const points = entry.points
                .map((point) => `${x(point.year)},${y(point.value)}`)
                .join(" ");
              return (
                <g key={`${entry.label}-${index}`}>
                  <polyline points={points} className="preview-line" style={{ stroke: color }} />
                  {entry.points.map((point) => (
                    <circle
                      key={`${entry.label}-${point.year}`}
                      cx={x(point.year)}
                      cy={y(point.value)}
                      r={3}
                      className="preview-dot"
                      style={{ fill: color }}
                      onMouseEnter={() =>
                        setTooltip(entry.label, point, entry.unit, x(point.year), y(point.value))
                      }
                    />
                  ))}
                </g>
              );
            })
          : null}
      </svg>

      <details className="preview-legend-panel" open={legendOpen}>
        <summary
          className="preview-legend-head"
          onClick={(event) => {
            event.preventDefault();
            setLegendOpen((current) => !current);
          }}
        >
          <span className="subtle">
            {effectiveChartMode === "stacked_area"
              ? "Stacked area chart"
              : effectiveChartMode === "stacked_bar"
                ? "Stacked bar chart"
                : effectiveChartMode === "bar"
                  ? "Grouped bar chart"
                  : "Line chart"}{" "}
            / {series.length} series plotted
          </span>
          <div className="action-cluster compact-actions">
            {hiddenLabels.length && onResetHiddenSeries ? (
              <button
                className="button subtle-button compact-button"
                type="button"
                onClick={(event) => {
                  event.preventDefault();
                  event.stopPropagation();
                  onResetHiddenSeries();
                }}
              >
                Show hidden series
              </button>
            ) : null}
            <span className="subtle">Toggle series list</span>
          </div>
        </summary>

        {legendOpen ? (
          <div className="preview-legend">
            {series.map((entry, index) => {
              const hidden = hiddenLabels.includes(entry.label);
              return (
                <button
                  className="preview-legend-item"
                  data-hidden={hidden ? "true" : "false"}
                  key={`${entry.label}-${index}`}
                  type="button"
                  onClick={() => onToggleSeries?.(entry.label)}
                >
                  <span
                    className="preview-swatch"
                    style={{ backgroundColor: PREVIEW_COLORS[index % PREVIEW_COLORS.length] }}
                  />
                  <span>{entry.label}</span>
                  {entry.unit ? <span className="subtle">{entry.unit}</span> : null}
                </button>
              );
            })}
          </div>
        ) : null}
      </details>

      {!visibleSeries.length ? (
        <div className="inline-note">
          All plotted series are hidden. Use the legend to show them again.
        </div>
      ) : null}
    </div>
  );
}

export function ExplorerValuesTable({ series }: { series: PlotSeries[] }) {
  const rowHeight = 36;
  const viewportHeight = 360;
  const overscan = 10;
  const [scrollTop, setScrollTop] = useState(0);
  const rows = useMemo(() => explorerTableRows(series), [series]);
  const totalRows = rows.length;
  const visibleStart = Math.max(Math.floor(scrollTop / rowHeight) - overscan, 0);
  const visibleEnd = Math.min(
    Math.ceil((scrollTop + viewportHeight) / rowHeight) + overscan,
    totalRows,
  );
  const topSpacerHeight = visibleStart * rowHeight;
  const bottomSpacerHeight = Math.max(totalRows - visibleEnd, 0) * rowHeight;

  useEffect(() => {
    setScrollTop(0);
  }, [series]);

  if (!series.length) {
    return (
      <div className="empty-state compact-empty">
        Choose one or more regions and variables to populate the values table.
      </div>
    );
  }

  const visibleRows = rows.slice(visibleStart, visibleEnd);

  return (
    <div
      className="table-wrap explorer-table-wrap"
      onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
    >
      <table className="compact-table explorer-values-table">
        <thead>
          <tr>
            <th scope="col">Year</th>
            {series.map((entry) => (
              <th key={entry.label} scope="col">
                {entry.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {topSpacerHeight > 0 ? (
            <tr className="explorer-table-spacer-row" aria-hidden="true">
              <td
                className="explorer-table-spacer-cell"
                colSpan={series.length + 1}
                style={{ height: `${topSpacerHeight}px` }}
              />
            </tr>
          ) : null}
          {visibleRows.map((row) => (
            <tr key={`explorer-year-${row.year}`}>
              <td>{row.year}</td>
              {series.map((entry) => (
                <td key={`${row.year}-${entry.label}`}>
                  {typeof row.values[entry.label] === "number"
                    ? row.values[entry.label].toLocaleString(undefined, {
                        maximumFractionDigits: 3,
                      })
                    : "—"}
                </td>
              ))}
            </tr>
          ))}
          {bottomSpacerHeight > 0 ? (
            <tr className="explorer-table-spacer-row" aria-hidden="true">
              <td
                className="explorer-table-spacer-cell"
                colSpan={series.length + 1}
                style={{ height: `${bottomSpacerHeight}px` }}
              />
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}
