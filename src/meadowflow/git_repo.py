import dataclasses

from meadowrun.meadowrun_pb2 import GitRepoCommit


@dataclasses.dataclass(frozen=True)
class GitRepo:
    """Represents a git repo"""

    # specifies the url, will be provided to git clone, see
    # https://git-scm.com/docs/git-clone
    repo_url: str

    default_branch: str

    # TODO this should actually be the name of a file in the repository that specifies
    #  what interpreter/libraries we should use. Currently it is just the path to the
    #  interpreter on the local machine
    interpreter_path: str

    def get_commit(self) -> GitRepoCommit:
        # TODO this seems kind of silly right now, but we should move the logic for
        #  converting from a branch name to a specific commit hash to this function from
        #  _get_git_repo_commit_interpreter_and_code so we can show the user what commit
        #  we actually ran with.
        # TODO also we will add the ability to specify overrides (e.g. a specific commit
        #  or an alternate branch)
        return GitRepoCommit(
            repo_url=self.repo_url,
            # TODO this is very sketchy. Need to invest time in all of the possible
            #  revision specifications
            commit="origin/" + self.default_branch,
            interpreter_path=self.interpreter_path,
        )
