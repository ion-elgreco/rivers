import rivers as rs

from package_demo.assets.assets import raw_data, summary

job = rs.Job(
    name="user_pipeline",
    assets=[raw_data, summary],
    executor=rs.Executor.in_process(),
)
