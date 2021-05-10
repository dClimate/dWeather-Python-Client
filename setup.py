import setuptools

try:
    # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:
    # for pip <= 9.0.3
    from pip.req import parse_requirements

def load_requirements(fname):
    reqs = parse_requirements(fname, session="test")
    try:
        return [str(ir.requirement) for ir in reqs]
    except AttributeError:
        return [str(ir.req) for ir in reqs]

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dweather_client",
    include_package_data=True,
    install_requires=load_requirements("requirements.txt"),
    version="2.1.20.dev3",
    author="Arbol",
    author_email="info@arbolmarket.com",
    description="Python client for interacting with weather data on IPFS.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Arbol-Project/dWeather-Python-Client.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
