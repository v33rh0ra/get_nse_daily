from setuptools import setup, find_packages
print("Packages found:",find_packages())

setup(
    name = "get_nse_daily",
    version="0.1",
    packages=find_packages(),
    author="Tanveer Hora",
    author_email = 'tanveer.hora@gmail.com',
    license='MIT',
    description="pure python library built using requests to get daily day end data from nse india",
    keywords = "nse,bhavcopy,bhav",
    install_requires = [
        'requests>=2.26.0'
    ],
    url = "https://github.com/v33rh0ra/get_nse_daily",#project home page from github
    download_url = 'https://github.com/v33rh0ra/get_nse_daily/archive/v_01.tar.gz',
    project_urls={
        "Documentation":"https://v33rh0ra.github.io/get_nse_daily/nse_daily/index.html",
        "Bug Tracker":"https://github.com/v33rh0ra/get_nse_daily/issues",
        "Source Code":"https://github.com/v33rh0ra/get_nse_daily",
    },
    classifiers=[
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 3',
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering",
        'License :: OSI Approved :: MIT License',   # Again, pick a license
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        'Intended Audience :: Developers',
    ],
)