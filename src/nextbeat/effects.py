"""
These functions should be called from inside of nextbeat jobs to indicate that an effect
has happened in that job.
"""

from nextbeat.jobs import Effects

# effects are processed in Scheduler._process_effects
# TODO this is a stub for future implementation, also need to add support in
#  LocalJobRunner
_effects = Effects()
