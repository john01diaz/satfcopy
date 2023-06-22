from subprocess import check_output, CalledProcessError


def get_git_root_path() \
        -> str:
    try:
        base = \
            check_output(
                'git rev-parse --show-toplevel',
                shell=True)

    except CalledProcessError:
        raise \
            IOError(
                'Current working directory is not a git repository')

    git_root_path = \
        base.decode('utf-8').strip()

    return \
        git_root_path
