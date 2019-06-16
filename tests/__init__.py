from mo_threads import Till

TIMEOUT = 10


def wait_until(index, condition):
    timeout = Till(seconds=TIMEOUT)
    while not timeout:
        if condition():
            break
        index.refresh()


def delete(index, filter):
    index.delete_record(filter)
    index.refresh()
    wait_until(
        index, lambda: index.search({"size": 0, "query": filter}).hits.total == 0
    )


def insert(index, records):
    ids = records.value._id
    index.extend(records)
    index.refresh()
    wait_until(
        index,
        lambda: index.search(
            {"size": 0, "query": {"terms": {"_id": ids}}}
        ).hits.total
        == len(records),
    )
