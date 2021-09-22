from setuptools import setup, find_packages
print("Packages found:",find_packages())

setup(
    name = "get_nse_daily",
    version="1.0.0",
    packages=find_packages(),
    author="tanveer.hora@gmail.com",
    description="",
    keywords = "",
    install_requires = [
        'requests>=2.26.0'
    ],
    url = "",#project home page from github
    project_urls={
        "Documentation":"",
        "Bug Tracker":"",
        "Source Code":"",
    },
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering",
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research"
    ],
)