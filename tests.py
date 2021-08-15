import asyncio

from unittest import IsolatedAsyncioTestCase
from aiocollections import RunningTasks


async def _empty_coro():
    pass


class RunningTaskTest(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.running_tasks = RunningTasks()
        return super().setUp()

    async def asyncSetUp(self) -> None:
        self.loop = asyncio.get_running_loop()
        return await super().asyncSetUp()

    async def test_create_task(self):
        """Test a creation of a new task"""
        ran = False

        async def coro():
            nonlocal ran
            ran = True

        task = self.running_tasks.create_task(coro)
        self.assertEqual(1, len(self.running_tasks))
        self.assertIsInstance(task, asyncio.Task)
        await task
        self.assertEqual(0, len(self.running_tasks))
        self.assertTrue(ran)

        future = self.loop.create_future()
        task = self.running_tasks.create_task(future)
        self.assertEqual(1, len(self.running_tasks))
        self.assertIs(task, future)
        future.set_result(None)
        self.assertEqual(0, len(self.running_tasks))

    async def test_add_task(self):
        """Test adding a running task"""
        task = asyncio.create_task(_empty_coro())
        self.running_tasks.add(task)
        self.assertEqual(1, len(self.running_tasks))
        await task
        self.assertEqual(0, len(self.running_tasks))

    async def test_discard_task(self):
        """Test discarding a running task"""
        task = asyncio.create_task(_empty_coro())
        self.running_tasks.add(task)
        self.assertEqual(1, len(self.running_tasks))
        self.running_tasks.discard(task)
        self.assertEqual(0, len(self.running_tasks))

    async def test_iter(self):
        """Test iteration over a running tasks"""
        task = asyncio.create_task(_empty_coro())
        self.running_tasks.add(task)
        self.assertEqual(1, len(self.running_tasks))
        self.assertEqual({task}, set(self.running_tasks))

    async def test_contains(self):
        """Test membership of a running tasks"""
        task = self.running_tasks.create_task(_empty_coro())
        self.running_tasks.add(task)
        self.assertEqual(1, len(self.running_tasks))
        self.assertTrue(task in self.running_tasks)

    async def test_multiple_tasks(self):
        """Test adding multiple tasks"""
        for _ in range(10):
            task = self.running_tasks.create_task(_empty_coro())
            self.running_tasks.add(task)
        self.assertEqual(10, len(self.running_tasks))
        await asyncio.gather(*self.running_tasks)
        self.assertEqual(0, len(self.running_tasks))

    async def test_add_done_handler(self):
        """Test adding a done handler"""
        async def coro():
            return 1

        result = 0

        def handler(task):
            nonlocal result
            result = task.result()

        task = self.running_tasks.create_task(coro())
        self.running_tasks.add_done_handler(handler)
        await task
        self.assertEqual(1, result)

    async def test_add_done_handler_error(self):
        """Test adding a done handler with an error"""
        async def coro():
            raise ValueError()

        exc = None

        def handler(task):
            nonlocal exc
            exc = task.exception()

        task = self.running_tasks.create_task(coro())
        self.running_tasks.add_done_handler(handler)
        await task
        self.assertIsInstance(exc, ValueError)

    async def test_multiple_done_handlers(self):
        """Test adding multiple done handlers"""
        async def coro():
            return 1

        result = []

        def handler(task):
            nonlocal result
            result.append(task.result())

        task = self.running_tasks.create_task(coro())
        self.running_tasks.add_done_handler(handler)
        self.running_tasks.add_done_handler(handler)
        await task
        self.assertEqual(result, [1, 1])

    async def test_remove_done_handler(self):
        """Test removing a done handler"""
        async def coro():
            return 1

        result = 0

        def handler(task):
            nonlocal result
            result = task.result()

        task = self.running_tasks.create_task(coro())
        self.running_tasks.add_done_handler(handler)
        num_removed = self.running_tasks.remove_done_handler(handler)
        await task
        self.assertEqual(0, result)
        self.assertEqual(1, num_removed)

    async def test_discarded_task_done_handler(self):
        """Make sure done handler is not called on a discarded task"""
        waiter = self.loop.create_future()
        continue_ = self.loop.create_future()

        async def coro():
            waiter.set_result(None)
            await continue_

        result = False

        def handler(task):
            nonlocal result
            result = True

        task = self.running_tasks.create_task(coro())
        self.running_tasks.add_done_handler(handler)
        await waiter
        continue_.set_result(None)
        self.running_tasks.discard(task)
        await task
        await asyncio.sleep(0)  # Make sure done handler has a chance to run
        self.assertFalse(result)

    async def test_wait(self):
        """Test wait() to see if all tasks are done"""
        async def coro():
            await asyncio.sleep(0)
            return 1

        tasks = [self.running_tasks.create_task(coro()) for _ in range(10)]
        await self.running_tasks.wait()
        self.assertEqual(0, len(self.running_tasks))
        for task in tasks:
            self.assertTrue(task.done())

    async def test_wait_task_discard(self):
        """Test wait() to see if it is called when a task is discarded"""
        future = self.loop.create_future()

        async def coro():
            await future

        task = self.running_tasks.create_task(coro())
        waiting = asyncio.create_task(self.running_tasks.wait())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertFalse(waiting.done())
        self.running_tasks.discard(task)
        await waiting
        self.assertEqual(0, len(self.running_tasks))

    async def test_multiple_simultaneous_wait(self):
        """Test to see if multiple wait()s work"""
        future = self.loop.create_future()

        async def coro():
            await future

        self.running_tasks.create_task(coro())
        waiting = asyncio.create_task(self.running_tasks.wait())
        waiting2 = asyncio.create_task(self.running_tasks.wait())

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertFalse(waiting.done())
        self.assertFalse(waiting2.done())

        future.set_result(None)
        await waiting
        await waiting2
        self.assertEqual(0, len(self.running_tasks))

    async def test_empty_wait(self):
        """Test to see if wait() works with no tasks"""
        await self.running_tasks.wait()
        self.assertEqual(0, len(self.running_tasks))
