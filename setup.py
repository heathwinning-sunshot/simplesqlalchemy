import setuptools

setuptools.setup(
    name="simplesqlalchemy",
    version="0.0.1",
    author="Heath Winning",
    description="A small wrapper around sqlalchemy to reduce boilerplate when using sqlalchemy and pandas.",
    packages=setuptools.find_packages(),
    python_requires=">=3.8",
)