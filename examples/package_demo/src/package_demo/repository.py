import rivers as rs

from package_demo.assets.assets import raw_data, summary
from package_demo.jobs.jobs import job

repo = rs.CodeRepository(
    assets=[raw_data, summary],
    jobs=[job],
)
