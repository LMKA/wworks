from functools import reduce

from wworks.swworks import WorkManager

TEST_DATA_LENGTH = 10
CHUNKS_LENGTH = 5

# Without args, without return
def my_test_work_1():
    pass

# Without args, with return
def my_test_work_2():
    return (0, '', False)

# With args, without return
def my_test_work_3(*args):
    pass

# With args, with return
def my_test_work_4(*args):
    return reduce(lambda x, y: x*y, args)

# Tests
## Work
def test_WorkManager_work_1():
    """Can apply a function without args nor return.
    """
    results = WorkManager("test_WorkManager_work_1").work("test_work_1", my_test_work_1, TEST_DATA_LENGTH)
    for (task_name, _, task_result) in results:
        assert my_test_work_1() == task_result

def test_WorkManager_work_2():
    """Can apply a function without args and with return.
    """
    results = WorkManager("test_WorkManager_work_2").work("test_work_2", my_test_work_2, TEST_DATA_LENGTH)
    for (task_name, _, task_result) in results:
        assert my_test_work_2() == task_result

def test_WorkManager_work_3():
    """Can apply a function with args and without return.
    """
    work_data = [(x, x) for x in range(TEST_DATA_LENGTH)]
    results = WorkManager("test_WorkManager_work_3").work("test_work_3", my_test_work_3, work_data)
    for (task_name, task_data, task_result) in results:
        assert my_test_work_3(*task_data) == task_result

def test_WorkManager_work_4():
    """Can apply a function with args and return.
    """
    work_data = [(x, x) for x in range(TEST_DATA_LENGTH)]
    results = WorkManager("test_WorkManager_work_4").work("test_work_4", my_test_work_4, work_data)
    for (task_name, task_data, task_result) in results:
        assert my_test_work_4(*task_data) == task_result
    

## Chunks
def test_WorkManager_chunks():
    """Can yield n-sized chunks from list.
    """
    work_data = [(x, x) for x in range(TEST_DATA_LENGTH)]
    results = WorkManager("test_WorkManager_chunks").chunks(work_data, CHUNKS_LENGTH)
    for i, chunk in enumerate(results):
        assert chunk == work_data[i*CHUNKS_LENGTH:(i + 1)*CHUNKS_LENGTH]


## Dispatch
def test_WorkManager_dispatch_1():
    """Can dispatch and apply a function without args nor return.
    """
    results = WorkManager("test_WorkManager_dispatch_1").dispatch(my_test_work_1, TEST_DATA_LENGTH, workload=CHUNKS_LENGTH)
    for (worker_name, worker_result) in results:
        for (task_name, _, task_result) in worker_result:
            assert my_test_work_1() == task_result

def test_WorkManager_dispatch_2():
    """Can dispatch and apply a function without args and with return.
    """
    results = WorkManager("test_WorkManager_dispatch_2").dispatch(my_test_work_2, TEST_DATA_LENGTH, workload=CHUNKS_LENGTH)
    for (worker_name, worker_result) in results:
        for (task_name, _, task_result) in worker_result:
            assert my_test_work_2() == task_result

def test_WorkManager_dispatch_3():
    """Can dispatch and apply a function with args and without return.
    """
    work_data = [(x, x) for x in range(TEST_DATA_LENGTH)]
    results = WorkManager("test_WorkManager_dispatch_3").dispatch(my_test_work_3, work_data, workload=CHUNKS_LENGTH)
    for (worker_name, worker_result) in results:
        for (task_name, task_data, task_result) in worker_result:
            assert my_test_work_3(*task_data) == task_result

def test_WorkManager_dispatch_4():
    """Can dispatch and apply a function with args and return.
    """
    work_data = [(x, x) for x in range(TEST_DATA_LENGTH)]
    results = WorkManager("test_WorkManager_dispatch_4").dispatch(my_test_work_4, work_data, workload=CHUNKS_LENGTH)
    for (worker_name, worker_result) in results:
        for (task_name, task_data, task_result) in worker_result:
            assert my_test_work_4(*task_data) == task_result
