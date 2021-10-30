from wworks.pools import WorkersPool, TasksPool
from functools import reduce

TEST_DATA_LENGTH = 10

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
## WorkersPool
def test_WorkersPool_1():
    """Can create WorkersPool and apply a function without args nor return.
    """
    results = WorkersPool().do('test_WorkersPool_1', my_test_work_1, TEST_DATA_LENGTH)
    for i, worker_result in enumerate(results):
        assert my_test_work_1() == worker_result

def test_WorkersPool_2():
    """Can create WorkersPool and apply a function without args and with return.
    """
    results = WorkersPool().do('test_WorkersPool_2', my_test_work_2, TEST_DATA_LENGTH)
    for i, worker_result in enumerate(results):
        assert my_test_work_2() == worker_result

def test_WorkersPool_3():
    """Can create WorkersPool and apply a function with args and without return.
    """
    work_data = [(x, x) for x in range(10)]
    results = WorkersPool().do('test_WorkersPool_3', my_test_work_3, work_data)
    for i, worker_result in enumerate(results):
        assert my_test_work_3(*work_data[i]) == worker_result

def test_WorkersPool_4():
    """Can create WorkersPool and apply a function with args and return.
    """
    work_data = [(x, x) for x in range(10)]
    results = WorkersPool().do('test_WorkersPool_4', my_test_work_4, work_data)
    for i, worker_result in enumerate(results):
        assert my_test_work_4(*work_data[i]) == worker_result

## TasksPool
def test_TasksPool_1():
    """Can create TasksPool and apply a function without args nor return
    """
    results = TasksPool().do('test_TasksPool_1', my_test_work_1, TEST_DATA_LENGTH)
    for i, task_result in enumerate(results):
        assert my_test_work_1() == task_result

def test_TasksPool_2():
    """Can create TasksPool and apply a function without args and with return.
    """
    results = TasksPool().do('test_TasksPool_2', my_test_work_2, TEST_DATA_LENGTH)
    for i, task_result in enumerate(results):
        assert my_test_work_2() == task_result

def test_TasksPool_3():
    """Can create TasksPool and apply a function with args and without return.
    """
    work_data = [(x, x) for x in range(10)]
    results = TasksPool().do('test_TasksPool_3', my_test_work_3, work_data)
    for i, task_result in enumerate(results):
        assert my_test_work_3(*work_data[i]) == task_result

def test_TasksPool_4():
    """Can create TasksPool and apply a function with args and return.
    """
    work_data = [(x, x) for x in range(10)]
    results = TasksPool().do('test_TasksPool_4', my_test_work_4, work_data)
    for i, task_result in enumerate(results):
        assert my_test_work_4(*work_data[i]) == task_result
