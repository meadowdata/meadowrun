# from itertools import chain
# from typing import Any, Callable, Optional, Union, overload

# from nextbeat.jobs import Actions, F, Job, JobStateChangeTrigger
# from nextbeat.scheduler import Schedulable, Scheduler


# @overload
# def job(func: F, /) -> Job[F]:
#     ...


# @overload
# def job(*, name: Optional[str] = None) -> Callable[[F], Job[F]]:
#     ...


# def job(
#     func: F = None, /, name: Optional[str] = None
# ) -> Union[Job, Callable[[F], Job[F]]]:
#     def wrapper(func: F) -> Job[F]:
#         return Job(func.__name__ if name is None else name, func)

#     if func is not None:
#         return wrapper(func)
#     else:
#         return wrapper


# default_scheduler: Scheduler = Scheduler()


# def schedule(job: Job, /, *args: Any, **kwargs: Any) -> Job:
#     deps = []
#     for arg in chain(args, kwargs.values()):
#         if isinstance(arg, Job):
#             if not default_scheduler.exists(job):
#                 schedule(job)
#             deps.append(arg)
#     triggers = tuple(
#         (JobStateChangeTrigger(j.name, ("succeeded",)), Actions.run) for j in deps
#     )
#     default_scheduler.add_job(Schedulable(job, triggers))
#     return job


# def run(job: Job) -> None:
#     default_scheduler.manual_run(job)


# @job
# def prev() -> None:
#     ...


# @job
# def test(dep: Job) -> None:
#     ...


# schedule(test, prev)
