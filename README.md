# WHAT-IF: Supporting water infrastructure investment planning with hydroeconomic optimization models 

**This is the example branch - perfect start point to create your own WHAT-IF case study !**

The WHAT-IF (Water, Hydropower, Agriculture Tool for Investment and Financing) model is an open source decision support tool 
distributed under the GPLv3 license and is described in the [HESS publication](https://www.hydrol-earth-syst-sci-discuss.net/hess-2019-167/):
> WHAT-IF: an open-source decision support tool for water infrastructure investment planning within the Water-Energy-Food-Climate Nexus  

## Wiki
Check the [wiki page](https://github.com/RaphaelPB/WHAT-IF/wiki) for all detailed information:
* [Code structure](https://github.com/RaphaelPB/WHAT-IF/wiki/Code-structure)
* [Installing and running WHAT IF](https://github.com/RaphaelPB/WHAT-IF/wiki/Installing-and-running-WHAT-IF)
* [Creating, running and comparing scenarios](https://github.com/RaphaelPB/WHAT-IF/wiki/Creating,-running-and-comparing-scenarios) 
* and many more...


## Usage

The [Installing and running WHAT IF](https://github.com/RaphaelPB/WHAT-IF/wiki/Installing-and-running-WHAT-IF) contains a step by step guide describing the process, in brief:
* Install the [Anaconda navigator](https://anaconda.org/anaconda/anaconda-navigator), 
the simpliest way to manage your pyhton packages and versions
* In the anaconda prompt, run:  
`conda config --add channels conda-forge`  
`conda create -n WHATIF_py37 python=3.7.3`  
`conda install -n WHATIF_py37 openpyxl xlsxwriter xlrd pyomo=5.6.2 pandas numpy multiprocess ipopt glpk`  
`conda activate WHATIF_py37`  

* Recommended solvers are ipopt (open-source, non-linear), cplex (free for academics, linear) 
* WHAT-IF is now only released with Python 3.7


## Contributors 
Payet-Burin Raphaël (DTU/COWI - rapp@env.dtu.dk)  
Mikkel Kromann (dansk energi)  
Peter Bauer-Gottwein (DTU - pbau@env.dtu.dk)
Silvio Pereira-Cardenal (COWI - sipa@cowi.com)
Kenneth Strzepek (MIT)
