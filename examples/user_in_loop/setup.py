from setuptools import find_packages, setup

setup(
    name="user_in_loop",
    version="0+dev",
    author_email="hello@elementl.com",
    packages=find_packages(exclude=["user_in_loop_tests*"]),
    include_package_data=True,
    install_requires=["dagster"],
    python_requires=">=3.6,<=3.10",
    author="Elementl",
    license="Apache-2.0",
    description="Dagster example for how to write a user-in-the-loop pipeline.",
    url="https://github.com/dagster-io/dagster/tree/master/examples/user_in_loop",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
