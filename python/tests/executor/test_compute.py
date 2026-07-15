"""Per-asset compute (``@Asset(compute=...)``).

Local suites only check acceptance and the inert-on-local-executors path;
the k3d integration suite asserts the rendered pod resources.
"""

import rivers as rs


def test_compute_accepted_and_inert_locally(storage):
    @rs.Asset(compute=rs.Compute(cpu="2", memory="1Gi"))
    def sized() -> int:
        return 1

    repo = rs.CodeRepository(assets=[sized], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    assert repo.materialize().success


def test_compute_on_asset_def(storage):
    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("cd_a", compute=rs.Compute(memory="2Gi")),
            rs.AssetDef("cd_b"),
        ],
    )
    def multi_sized():
        return {"cd_a": 1, "cd_b": 2}

    repo = rs.CodeRepository(
        assets=[multi_sized], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    assert repo.materialize().success


def test_compute_axes_and_repr():
    c = rs.Compute(cpu="500m", memory="32Gi", gpu="1")
    assert c.cpu == "500m"
    assert c.memory == "32Gi"
    assert c.gpu == "1"
    assert "memory='32Gi'" in repr(c)
