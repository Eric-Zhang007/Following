from __future__ import annotations


def remaining_tp_weights(all_tp_points: list[float], active_tp_points: list[float]) -> list[float]:
    normalized_all = [float(v) for v in all_tp_points if float(v) > 0]
    normalized_active = [float(v) for v in active_tp_points if float(v) > 0]
    if not normalized_active:
        return []

    base_weights = _base_tp_weights(len(normalized_all) or len(normalized_active))
    if len(normalized_all) != len(base_weights):
        normalized_all = normalized_active
        base_weights = _base_tp_weights(len(normalized_active))

    selected: list[float] = []
    remaining_points = list(normalized_active)
    for tp, weight in zip(normalized_all, base_weights, strict=False):
        for idx, candidate in enumerate(remaining_points):
            if _tp_matches(tp, candidate):
                selected.append(float(weight))
                remaining_points.pop(idx)
                break

    if len(selected) != len(normalized_active):
        selected = _base_tp_weights(len(normalized_active))

    total = sum(selected)
    if total <= 0:
        return _base_tp_weights(len(normalized_active))
    return [float(weight) / float(total) for weight in selected]


def _base_tp_weights(tp_count: int) -> list[float]:
    if tp_count <= 0:
        return []
    if tp_count == 1:
        return [1.0]
    if tp_count == 2:
        return [0.5, 0.5]
    if tp_count == 3:
        return [0.35, 0.35, 0.30]
    return [1.0 / float(tp_count)] * tp_count


def _tp_matches(left: float, right: float) -> bool:
    tolerance = max(1e-9, max(abs(float(left)), abs(float(right)), 1.0) * 1e-6)
    return abs(float(left) - float(right)) <= tolerance
