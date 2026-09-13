"""Bounded, ordered reporting with independent Python worker processes.

Workers start this module, never the caller's script. The parent validates and
snapshots inputs, preserves activity pairing order, and merges summaries and
audit fragments in that same order. Temporary files are private to the session.
"""

from __future__ import annotations

import hashlib
import heapq
import os
import pickle
import subprocess
import sys
import tempfile
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from . import change_report as r
from ._report_inputs import ReportInput, write_report_input


def _locators(headers):
    occurrences = Counter()
    result = []
    counts = {}
    for activity_id, code, semantic, count in headers:
        result.append(
            r._ActivityLocator(
                activity_id, code, semantic, "", occurrence=occurrences[semantic]
            )
        )
        occurrences[semantic] += 1
        counts[activity_id] = count
    return result, counts


def _partition_scenarios(all_pairs, weights, fragments):
    """Keep semantic ranges in the same worker across all scenarios."""
    budget = max(sum(weights.values()) / fragments, 1)
    assignments = {}
    count = 0
    for semantic, weight in sorted(weights.items()):
        assignments[semantic] = min(int(count // budget), fragments - 1)
        count += weight
    for pairs in all_pairs:
        blocks = [[] for _ in range(fragments)]
        for pair in pairs:
            blocks[assignments[(pair[1] or pair[0]).semantic_key]].append(pair)
        yield blocks


def _merge_summary(target, part):
    for key, count in part.scenario_counts.items():
        target.scenario_counts[key].update(count)
    for key, count in part.sector_counts.items():
        target.sector_counts[key].update(count)
    for row in part.market_rows:
        old_shares, new_keys = row.pop("_dropped_shares")
        # Match the caller's set iteration and floating-point summation even
        # when worker interpreters have independent randomized hash seeds.
        row["dropped share"] = sum(
            old_shares[key] for key in set(old_shares) - set(new_keys)
        )
        target.market_rows.append(row)
    target.fallback_counts.update(part.fallback_counts)
    target.fallback_payloads.update(part.fallback_payloads)
    for key, heap in part.key_heaps.items():
        combined = target.key_heaps[key] + [
            (score, count + target._heap_counter, row) for score, count, row in heap
        ]
        target.key_heaps[key] = heapq.nlargest(20, combined, key=lambda x: x[:2])
        heapq.heapify(target.key_heaps[key])
    target._heap_counter += part._heap_counter


def _dump(value, path):
    temporary = path.with_suffix(".tmp")
    with temporary.open("wb") as stream:
        pickle.dump(value, stream, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(temporary, path)


def _compare_block(
    directory, part, order, data, job, source, final, pairs, attribution
):
    summary = r._Summary()
    summary._defer_market_shares = True
    sink = r._ParquetSink(directory / f"audit-{order}-{part}.parquet")
    final_hashes = {}
    source_hashes = {}
    for old, new in pairs:
        if old is not None:
            r._prepare_locator(source, old)
            if order == 0:
                summary.source_exchange_count += len(old.payload["exchanges"])
            if not job["source_fingerprint"]:
                source_hashes[old.activity_id] = old.visible_hash
        if new is not None:
            r._prepare_locator(final, new)
            final_hashes[new.activity_id] = new.visible_hash
        records = r._activity_records(
            source,
            final,
            (old, new),
            report_id=job["report_id"],
            build_id=job["build_id"],
            scenario_order=order,
            scenario_label=data["label"],
            source_fingerprint=job["source_fingerprint"],
            final_fingerprint="",
            certificate_key=data["certificate"],
            attribution_index=attribution,
            summary=summary,
        )
        summary.consume_activity(records)
        sink.write(records)
        if new is not None:
            new.payload = None
    sink.close()
    _dump(
        (summary, final_hashes, source_hashes),
        directory / f"result-{order}-{part}.pickle",
    )


def _worker(directory, part):
    r.pa.set_cpu_count(1)
    r.pa.set_io_thread_count(1)
    with (directory / f"job-{part}.pickle").open("rb") as stream:
        job = pickle.load(stream)
    source = ReportInput(directory / "source")
    try:
        for order, data in enumerate(job["scenarios"]):
            final = ReportInput(directory / f"final-{order}")
            attribution = object.__new__(r._AttributionIndex)
            attribution.by_code, attribution.by_semantic = data["attribution"]
            try:
                for block, pairs in data["blocks"]:
                    _compare_block(
                        directory,
                        block,
                        order,
                        data,
                        job,
                        source,
                        final,
                        pairs,
                        attribution,
                    )
            finally:
                final.close()
    finally:
        source.close()


def _append_audits(
    writer, directory, order, workers, source_fingerprint, final_fingerprint
):
    for part in range(workers):
        path = directory / f"audit-{order}-{part}.parquet"
        for batch in r.pq.ParquetFile(path).iter_batches(batch_size=65536):
            table = r.pa.Table.from_batches([batch])
            for name, value in (
                ("source_fingerprint", source_fingerprint),
                ("final_fingerprint", final_fingerprint),
            ):
                position = r.DETAIL_SCHEMA.get_field_index(name)
                table = table.set_column(
                    position,
                    r.DETAIL_SCHEMA.field(position),
                    r.pa.array([value] * len(table)),
                )
            writer.write_table(table, row_group_size=r.DETAIL_BATCH_SIZE)


def generate_details_parallel(
    *,
    source_store,
    scenarios,
    details_path,
    report_id,
    build_id,
    source_fingerprint,
    workers,
):
    """Produce the exact ordered audit, with bounded worker memory."""
    with tempfile.TemporaryDirectory(
        prefix=".premise-report-", dir=details_path.parent
    ) as name:
        directory = Path(name)
        source_index, source_counts = _locators(
            write_report_input(source_store, directory / "source")
        )
        summary = r._Summary(
            source_activity_count=len(source_index),
            source_exchange_count=sum(source_counts.values()),
        )
        finals = []
        scenario_events = []
        all_pairs = []
        weights = Counter()
        fragments = workers * 4
        jobs = [
            dict(
                report_id=report_id,
                build_id=build_id,
                source_fingerprint=source_fingerprint,
                scenarios=[],
            )
            for _ in range(workers)
        ]
        for order, scenario in enumerate(scenarios):
            final_index, final_counts = _locators(
                write_report_input(scenario.store, directory / f"final-{order}")
            )
            finals.append(final_index)
            label = r._scenario_label(scenario.identity)
            summary.final_counts[label] = len(final_index), sum(final_counts.values())
            attribution = r._AttributionIndex(scenario)
            scenario_events.append(attribution.events)
            pairs = r._pair_activities(source_index, final_index)
            all_pairs.append(pairs)
            for old, new in pairs:
                weights[(new or old).semantic_key] += (
                    1
                    + (source_counts[old.activity_id] if old else 0)
                    + (final_counts[new.activity_id] if new else 0)
                )
            for part, job in enumerate(jobs):
                job["scenarios"].append(
                    dict(
                        label=label,
                        certificate=r._certificate_key(scenario.validation_report),
                        attribution=(attribution.by_code, attribution.by_semantic),
                        blocks=[],
                    )
                )
        for order, blocks in enumerate(
            _partition_scenarios(all_pairs, weights, fragments)
        ):
            for part, job in enumerate(jobs):
                data = job["scenarios"][order]
                data["blocks"] = [
                    (block, blocks[block]) for block in range(part, fragments, workers)
                ]
                identities = [
                    new or old for _, pairs in data["blocks"] for old, new in pairs
                ]
                codes = {item.code for item in identities if item.code is not None}
                semantics = {item.semantic_key[:3] for item in identities}
                by_code, by_semantic = data["attribution"]
                data["attribution"] = (
                    {key: by_code[key] for key in codes if key in by_code},
                    {key: by_semantic[key] for key in semantics if key in by_semantic},
                )
        for part, job in enumerate(jobs):
            _dump(job, directory / f"job-{part}.pickle")
        del jobs
        environment = dict(
            os.environ,
            OPENBLAS_NUM_THREADS="1",
            OMP_NUM_THREADS="1",
            MKL_NUM_THREADS="1",
        )
        # Resolve the same package checkout even when called from a notebook or
        # an entry point whose working directory is elsewhere.
        package_root = str(Path(__file__).resolve().parents[1])
        environment["PYTHONPATH"] = os.pathsep.join(
            filter(None, (package_root, environment.get("PYTHONPATH")))
        )
        children = []
        logs = []
        try:
            for part in range(workers):
                log = (directory / f"worker-{part}.log").open("wb")
                logs.append(log)
                children.append(
                    subprocess.Popen(
                        [
                            sys.executable,
                            "-m",
                            "premise._report_parallel",
                            str(directory),
                            str(part),
                        ],
                        env=environment,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                    )
                )
            with r.pq.ParquetWriter(
                directory / "audit.parquet", r.DETAIL_SCHEMA, compression="zstd"
            ) as writer:
                # Reading/compressing the preceding scenario overlaps the next
                # scenario's Python comparisons, without concurrent writer use.
                with ThreadPoolExecutor(max_workers=1) as merger:
                    pending = None
                    for order, scenario in enumerate(scenarios):
                        parts = []
                        hashes = {}
                        old_hashes = {}
                        for part in range(fragments):
                            child = children[part % workers]
                            result_path = directory / f"result-{order}-{part}.pickle"
                            while not result_path.exists():
                                for index, process in enumerate(children):
                                    if process.poll() not in (None, 0):
                                        error = (
                                            directory / f"worker-{index}.log"
                                        ).read_text(errors="replace")
                                        raise RuntimeError(
                                            f"Change-report worker failed:\n{error[-4000:]}"
                                        )
                                if child.poll() == 0 and not result_path.exists():
                                    raise RuntimeError(
                                        "Change-report worker exited without its result."
                                    )
                                time.sleep(0.02)
                            with result_path.open("rb") as stream:
                                part_summary, final_hashes, source_hashes = pickle.load(
                                    stream
                                )
                            parts.append(part_summary)
                            hashes.update(final_hashes)
                            old_hashes.update(source_hashes)
                        if not source_fingerprint:
                            source_fingerprint = hashlib.sha256(
                                "".join(
                                    old_hashes[item.activity_id]
                                    for item in source_index
                                ).encode("ascii")
                            ).hexdigest()
                        fingerprint = hashlib.sha256(
                            "".join(
                                hashes[item.activity_id] for item in finals[order]
                            ).encode("ascii")
                        ).hexdigest()
                        label = r._scenario_label(scenario.identity)
                        summary.final_fingerprints[label] = fingerprint
                        r._collect_event_summaries(
                            summary, scenario, label, events=scenario_events[order]
                        )
                        scenario_events[order] = None
                        for part_summary in parts:
                            _merge_summary(summary, part_summary)
                        if order == 0:
                            summary.source_exchange_count = sum(
                                part_summary.source_exchange_count
                                for part_summary in parts
                            )
                        if pending is not None:
                            pending.result()
                        pending = merger.submit(
                            _append_audits,
                            writer,
                            directory,
                            order,
                            fragments,
                            source_fingerprint,
                            fingerprint,
                        )
                    if pending is not None:
                        pending.result()
            for child in children:
                if child.wait():
                    raise RuntimeError(
                        "Change-report worker failed after writing its result."
                    )
            os.replace(directory / "audit.parquet", details_path)
        finally:
            for child in children:
                if child.poll() is None:
                    child.terminate()
            for child in children:
                child.wait()
            for log in logs:
                log.close()
        return summary


if __name__ == "__main__":
    profile_directory = os.environ.get("PREMISE_REPORT_PROFILE_DIR")
    if profile_directory:
        import cProfile

        profiler = cProfile.Profile()
        try:
            profiler.runcall(_worker, Path(sys.argv[1]), int(sys.argv[2]))
        finally:
            profiler.dump_stats(
                str(Path(profile_directory) / f"worker-{os.getpid()}.pstats")
            )
    else:
        _worker(Path(sys.argv[1]), int(sys.argv[2]))
