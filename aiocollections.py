__author__ = "Bar Harel"
__version__ = "0.1.0"
__license__ = "MIT License"
__all__ = ["RunningTasks"]

from asyncio import ensure_future
from asyncio.futures import isfuture
from contextvars import copy_context, Context
from typing import (
    Any, Awaitable, Callable, Iterable,
    Iterator, List, MutableSet, Optional,
    Set, Tuple, TypeVar, overload)
from asyncio import Task, get_running_loop, Future

_F = TypeVar("_F", bound=Future)


class RunningTasks(MutableSet[Future]):
    """A set of actively running asyncio tasks or futures.

    Tasks can be added and will be automatically removed
    from this set when done.
    Calling wait() will wait until all tasks are done.

    Warning: This set may change while iterating over it
    if any task is done in the background.
    """

    def __init__(self, it: Iterable[Future] = None) -> None:
        self._tasks: Set[Future] = set(it or ())
        self._waiter: Optional[Future] = None
        self._callbacks: List[
            Tuple[Callable[[Future], Any], Context]] = []

    def add(self, task: Future) -> None:
        """Add a task to the set of active tasks."""
        if not isfuture(task):
            raise TypeError(
                f"task must be a Future object, not {type(task)!r}. "
                f"Have you meant {self.__class__.__name__}.create_task()?")
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def discard(self, task: Future) -> None:
        """Remove a task from the set of active tasks."""
        if not isfuture(task):
            raise TypeError("task must be a Future object.")
        self._tasks.discard(task)
        task.remove_done_callback(self._task_done)
        self._wakeup()  # Check if there are no more tasks

    def add_done_handler(self, fn: Callable[[Future], Any], *,
                         context: Optional[Context] = None) -> None:
        """Add a callback to be run when EACH of the tasks becomes done.

        The callback is called with a single argument - the future object.
        Useful for implementing exception handlers or retrieving results.

        Args:
            fn: The callback function to be called when a task is done.
            context: Optionally, the contextvars context to run the
            callback in. Defaults to the current context.
        """
        if context is None:
            context = copy_context()
        self._callbacks.append((fn, context))

    def remove_done_handler(self, fn: Callable[[Future], Any]) -> int:
        """Remove all instances of a callback from the "call when done" list.

        Args:
            fn: The callback to remove.

        Returns:
            The number of callbacks removed.
        """
        filtered_callbacks = [(f, ctx)
                              for (f, ctx) in self._callbacks
                              if f != fn]
        removed_count = len(self._callbacks) - len(filtered_callbacks)
        if removed_count:
            self._callbacks[:] = filtered_callbacks
        return removed_count

    def __iter__(self) -> Iterator[Future]:
        """Iterate over all tasks"""
        return iter(self._tasks)

    def __len__(self) -> int:
        """Return the number of tasks in the set"""
        return len(self._tasks)

    def __contains__(self, task: Future) -> bool:
        """Check if a task is in the set of active tasks"""
        return task in self._tasks

    def _wakeup(self):
        """Check if all tasks are done and we can wakeup the waiter"""
        if not self._tasks and (waiter := self._waiter):
            self._waiter = None
            waiter.set_result(None)

    def _task_done(self, task: Future) -> None:
        """Called when a task is done.

        Removes the task from the set of active tasks
        and runs any callbacks
        """
        self._tasks.discard(task)
        loop = get_running_loop()
        for (fn, context) in self._callbacks:
            loop.call_soon(fn, task, context=context)
        self._wakeup()

    @overload
    def create_task(self, awaitable: _F) -> _F:
        ...

    @overload
    def create_task(self, awaitable: Awaitable) -> Task:
        ...

    def create_task(self, awaitable):
        """Schedule an awaitable to be executed.

        Calls ensure_future and adds the task.

        Args:
            awaitable: The awaitable to be executed.

        Returns:
            A future representing the execution of the awaitable.
            If a task was given, it is returned.
        """
        task = ensure_future(awaitable)
        self.add(task)
        return task

    async def wait(self) -> None:
        """Wait for all tasks to finish.

        Note: If tasks are added while waiting, in rare cases
        the new tasks might not be waited for. This behavior
        is intended.
        """
        if not self._tasks:
            return
        if self._waiter is None:
            self._waiter = get_running_loop().create_future()
        # If a task was added after waiter.set_result() was scheduled we
        # will still have pending tasks when wait() returns. This should
        # not happen frequently unless a "done_handler" schedules a task
        # to run. For simplicity and optimization reasons, that edge case
        # is left unchecked.
        await self._waiter
