# wworks
> a light multiprocessing/multithreading work dispatcher for python.

[![GitHub version](https://badge.fury.io/gh/LMKA%2Fwworks.svg)](https://badge.fury.io/gh/LMKA%2Fwworks)
[![Python compatible versions](https://img.shields.io/badge/python-3.7%20|%203.8%20|%203.9-0b7cbc.svg)](https://shields.io/)
[![GitHub License](https://badgen.net/github/license/LMKA/wworks)](https://github.com/LMKA/wworks/blob/master/LICENSE)

wworks (Wrapped Works) is a work manager that uses both ProcessPoolExecutor and ThreadPoolExecutor to dispatch work by workload over processes and/or threads.

---
## Installation

Check your python version (must be >= 3.7)

```sh
> python --version
Python 3.8.12
```

Install wworks package

```sh
> pip install wworks
```

---
## Usage examples

<details open> 
    <summary markdown="span"> WorkManager.<b>work</b>(<b>work_name</b>, <b>work_to_do</b>, <b>work_data</b>) </summary>
</br>

> Use WorkManager.<b>work</b> to make multithreading for given function.
    
```python
from wworks.swworks import WorkManager

def multiply(x, y):
    return x*y

# Build 10 tuples from (0, 0) to (9, 9)
work_data = [(x, x) for x in range(10)]
results = WorkManager().work("multiply", multiply, work_data)
for (task_name, task_data, task_result) in results:
    print(f"{task_name} : multiply{task_data} => {task_result}")
```

```
Task #0 : multiply(0, 0) => 0
Task #1 : multiply(1, 1) => 1
Task #2 : multiply(2, 2) => 4
Task #3 : multiply(3, 3) => 9
Task #4 : multiply(4, 4) => 16
Task #5 : multiply(5, 5) => 25
Task #6 : multiply(6, 6) => 36
Task #7 : multiply(7, 7) => 49
Task #8 : multiply(8, 8) => 64
Task #9 : multiply(9, 9) => 81
```
In this case, WorkManager create 10 tasks (threads) to process.

</details>

<details open> 
    <summary markdown="span"> WorkManager.<b>chunks</b>(<b>lst</b>, <b>n</b>) </summary>
</br>

> Use WorkManager.<b>chunks</b> to yield n-sized chunks from lst.

```python
from wworks.swworks import WorkManager

# Build 10 tuples from (0, 0) to (9, 9)
work_data = [(x, x) for x in range(10)]
results = WorkManager().chunks(work_data, 4)
for i, chunk in enumerate(results):
    print(f"Chunk #{i}")
    print(chunk)
```

```
Chunk #0
[(0, 0), (1, 1), (2, 2), (3, 3)]
Chunk #1
[(4, 4), (5, 5), (6, 6), (7, 7)]
Chunk #2
[(8, 8), (9, 9)]
```
In this case, WorkManager yield 4-chunks from all provided tuples.

</details>

<details open> 
    <summary markdown="span"> WorkManager.<b>dispatch</b>(<b>work_to_do</b>, <b>work_data</b>, <b>workload</b>=4) </summary>
</br>

> Use WorkManager.<b>dispatch</b> to make chunked-by-process, multithreading for given function.
```python
from wworks.swworks import WorkManager

def multiply(x, y):
    return x*y

# Build 10 tuples from (0, 0) to (9, 9)
work_data = [(x, x) for x in range(10)]
results = WorkManager().dispatch(multiply, work_data)
for (worker_name, worker_result) in results:
    print(worker_name)
    for (task_name, task_data, task_result) in worker_result:
        print(f" - {task_name} : multiply{task_data} => {task_result}")
```

```
Worker #0
 - Task #0 : multiply(0, 0) => 0
 - Task #1 : multiply(1, 1) => 1
 - Task #2 : multiply(2, 2) => 4
 - Task #3 : multiply(3, 3) => 9
Worker #1
 - Task #0 : multiply(4, 4) => 16
 - Task #1 : multiply(5, 5) => 25
 - Task #2 : multiply(6, 6) => 36
 - Task #3 : multiply(7, 7) => 49
Worker #2
 - Task #0 : multiply(8, 8) => 64
 - Task #1 : multiply(9, 9) => 81
```
In this case, WorkManager create 3 workers (processes) and give each of them chunked work data respectivelly 4, 4 and 2 tasks to process.

</details>

---
## Release History

[![PyPI Package version](https://badgen.net/pypi/v/wworks)](https://pypi.org/project/wworks)
[![GitHub Lastest tag](https://badgen.net/github/tag/LMKA/wworks)](https://github.com/LMKA/wworks/tags)
[![GitLab Tags](https://badgen.net/github/tags/LMKA/wworks/)](https://github.com/LMKA/wworks/tags)


* 0.1.0
    * First version of wworks package

---
## Contributing


[![GitHub Forks](https://badgen.net/github/forks/LMKA/wworks)](https://github.com/LMKA/wworks/network/members)
[![GitHub Issues](https://badgen.net/github/issues/LMKA/wworks)](https://github.com/LMKA/wworks/issues)
[![GitHub PRs](https://badgen.net/github/prs/LMKA/wworks)](https://github.com/LMKA/wworks/pulls)
[![GitHub Open Issues](https://badgen.net/github/open-issues/LMKA/wworks)](https://GitHub.com/LMKA/wworks/issues?q=is%3Aopen)
[![GitHub Open PRs](https://badgen.net/github/open-prs/LMKA/wworks)](https://github.com/LMKA/wworks/issues?q=is%3Aopen)
[![GitHub Contributors](https://badgen.net/github/contributors/LMKA/wworks)](https://github.com/LMKA/wworks/graphs/contributors)
[![GitHub Stars](https://badgen.net/github/stars/LMKA/wworks)](https://github.com/LMKA/wworks/stargazers)

1. Fork it (<https://github.com/LMKA/wworks/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request

---
## Meta

[![Twitter Profile url](https://img.shields.io/twitter/url/https/twitter.com/bukotsunikki.svg?style=social&label=%40melakbir)](https://twitter.com/melakbir)


Distributed under the MIT license. See [LICENSE](https://github.com/LMKA/wworks/blob/master/LICENSE) for more information.


- <b>github</b>: [https://github.com/LMKA/wworks](https://github.com/LMKA/wworks)

- <b>pypi</b>: [https://pypi.org/project/wworks](https://pypi.org/project/wworks)

