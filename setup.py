import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dweather_client",
    version="1.0",
    author="Arbol Inc",
    author_email="info@arbolmarket.com",
    description="Python client for interacting with weather data on IPFS.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Arbol-Project/dWeather-Python-Client.git",
    packages=['dweather_client'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
