import ray
from datafusion_ray import DFRayContext, df_ray_runtime_env

ray.init(runtime_env=df_ray_runtime_env)

def main():
    ctx = DFRayContext()
    ctx.register_csv(
        "aggregate_test_100",
        "https://github.com/apache/arrow-testing/raw/master/data/csv/aggregate_test_100.csv",
    )

    df = ctx.sql("SELECT c1,c2,c3 FROM aggregate_test_100 LIMIT 5")

    df.show()
    print("Shown")

    df = ctx.sql("SELECT * FROM (SELECT * FROM VALUES (1), (2), (3) UNION ALL SELECT * FROM VALUES (4), (5)) WHERE column1 > 1")
    print(df.execution_plan())

    df.streaming_show()

try:
    main()
except Exception as e:
    print(e)
