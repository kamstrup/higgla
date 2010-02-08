from distutils.core import setup

# List of additional files to install
files = []

setup(name = "higgla",
    version = "0.0.1",
    description = "Client library for interacting with a Higgla server",
    author = "Mikkel Kamstrup Erlandsen",
    author_email = "mke@statsbiblioteket.dk",
    url = "http://github.com/mkamstrup/higgla",
    packages = ["higgla"],
    package_data = {'package' : files },
    scripts = [],
    long_description = """FIXME: Write me"""
)