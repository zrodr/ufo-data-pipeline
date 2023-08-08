import pytest

from os import pardir
from os.path import join
from tempfile import TemporaryDirectory

from datapipeline.application import Application

@pytest.fixture(scope="module")
def outdir():
    with TemporaryDirectory(dir=join(pardir, "tests")) as tmpdir:
         yield tmpdir
      

@pytest.fixture
def context(request):
    """ 
    Returns:
        a tuple containing a string file path and DataLoadStrategy subclass
    """
    app = Application(request.param)
    return app.program_context


# valid program arguments for the main method
@pytest.mark.parametrize(
    ("context", "expected"),
    [
        (["-f", "csv", "test_csv"], ("csv", "CSVLoadStrategy")),
        (["-f", "json", "test_json"], ("json", "JSONLoadStrategy")),
        (["-f", "xlsx", "test_excel"], ("xlsx", "ExcelLoadStrategy")),
        (["-f", "sqlite", "test_sqlite"], ("sqlite", "SQLiteLoadStrategy")),
        (["test_default"], ("xlsx", "ExcelLoadStrategy")),
    ],
    indirect=["context"]
)
def test_program_context_valid_args(outdir, context, expected):
    file_ext, loader_class = expected
    outfile, loader = context

    target = loader(join(outdir, outfile))

    # correct file format
    assert (outfile[1 + outfile.rfind(".") :]) == file_ext
    # make sure the path is correct
    assert target.resource_name.endswith(outfile)
    # check against class names to ensure the right load strategy is returned
    assert type(target).__name__ == loader_class


# TODO: invalid program arguments that will cause an exception to be thrown
@pytest.mark.parametrize(
    ("context", "expected_err"),
    [
        (["-q", "whoami", "-t", "h", "test_csv"], Exception),
    ],
    indirect=["context"]
)
def program_context_invalid_args(outdir, context, expected_err):
    file_ext, except_class = expected_err
    
    with pytest.raises(except_class) as err:
        outfile, loader = context
    
    assert "unrecognized arguments: " in str(err.value)


# TODO: valid arguments with values that will slip through the error checker
@pytest.mark.parametrize(
    ("context", "expected"),
    [
        (["fname\\<>.bad/format"], ("xlsx", "ExcelLoadStrategy")),
    ],
    indirect=["context"]
)
def program_context_valid_args_bad_value(outdir, context, expected):
    file_ext, loader_class = expected
    outfile, loader = context

    target = loader(join(outdir, outfile))

    invalid_chars = '/\\:*?\"<>|'

    # make sure the file name does not contain any invalid characters
    contains_invalid_char = not any(char in outfile for char in invalid_chars)
    assert contains_invalid_char

