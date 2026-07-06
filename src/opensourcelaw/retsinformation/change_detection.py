from __future__ import annotations

from .models import ChangedRawFetch, RawFetch


def classify_raw_fetches(current_fetches: list[RawFetch], historical_fetches: list[RawFetch]) -> list[ChangedRawFetch]:
    previous_by_item = _latest_successful_fetch_by_item(historical_fetches)
    changes: list[ChangedRawFetch] = []
    for fetch in current_fetches:
        previous = previous_by_item.get((fetch.source_id, fetch.external_id))
        previous_hash = previous.content_hash if previous else None
        if fetch.error:
            status = "failed"
        elif previous_hash is None:
            status = "new"
        elif previous_hash == fetch.content_hash:
            status = "unchanged"
        else:
            status = "changed"

        changes.append(
            ChangedRawFetch(
                source_id=fetch.source_id,
                external_id=fetch.external_id,
                fetched_at=fetch.fetched_at,
                status=status,
                content_hash=fetch.content_hash,
                previous_content_hash=previous_hash,
                raw_uri=fetch.raw_uri,
                run_id=fetch.run_id,
                metadata={"error": fetch.error} if fetch.error else {},
            )
        )
    return changes


def _latest_successful_fetch_by_item(fetches: list[RawFetch]) -> dict[tuple[str, str], RawFetch]:
    latest: dict[tuple[str, str], RawFetch] = {}
    for fetch in sorted(fetches, key=lambda item: item.fetched_at):
        if fetch.error or not fetch.content_hash:
            continue
        latest[(fetch.source_id, fetch.external_id)] = fetch
    return latest
