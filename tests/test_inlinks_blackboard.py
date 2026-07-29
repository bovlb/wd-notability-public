from wd_notability.inlinks.blackboard import InlinksBlackboard


def test_apply_interest_refreshes_seen_time_without_evicting_active_qids():
    blackboard = InlinksBlackboard()

    assert blackboard.apply_interest(["Q1"], observed_at=10) is True
    assert blackboard.apply_interest(["Q1"], observed_at=12) is False

    snapshot = blackboard.snapshot()
    assert set(snapshot) == {"Q1"}
    assert snapshot["Q1"].interest_seen_at == 12


def test_stale_interest_is_pruned_after_grace_window():
    blackboard = InlinksBlackboard()

    blackboard.apply_interest(["Q1", "Q2"], observed_at=10)

    assert blackboard.prune_stale_interest(stale_after_seconds=4, observed_at=13) == []
    assert set(blackboard.snapshot()) == {"Q1", "Q2"}

    assert blackboard.prune_stale_interest(stale_after_seconds=4, observed_at=14) == ["Q1", "Q2"]
    assert blackboard.snapshot() == {}
