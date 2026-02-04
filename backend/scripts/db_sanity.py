from backend.db import get_client


def main() -> None:
    sb = get_client()

    cols = sb.rpc("get_columns", {"p_table": "ingest_runs"}).execute().data or []
    names = {c["column_name"] for c in cols}
    assert "started_at" in names, f"ingest_runs missing started_at; has: {sorted(names)}"

    r = (
        sb.table("ingest_runs")
        .select("id,job_name,started_at,ok")
        .order("started_at", desc=True)
        .limit(3)
        .execute()
    )
    print("ingest_runs latest:", r.data)

    s = sb.table("sources").select("id,key").limit(1).execute()
    print("sources ok:", s.data)


if __name__ == "__main__":
    main()
