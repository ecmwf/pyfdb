Welcome to pyfdb's documentation!
=================================

The FDB (Fields DataBase) is a domain-specific object store developed at ECMWF for 
storing, indexing and retrieving GRIB data. Each GRIB message is stored as a 
field and indexed trough semantic metadata (i.e. physical variables such as 
temperature, pressure, ...). A set of fields can be retrieved specifying a
request using a specific language developed for accessing MARS Archive.

The documentation is divided into three parts: 

:ref:`architectural-introduction-label`
***************************************

TODO

:ref:`technical-introduction-label`
***************************************

TODO

:ref:`operational-introduction-label`
***************************************

TODO

.. index:: Structure

.. toctree::
   :maxdepth: 2
   :caption: Structure

   .. content/architectural-introduction
   content/technical-introduction
   .. content/operational-introduction

.. raw:: html
   <hr>

.. toctree::
   :maxdepth: 2
   :caption: Misc

   content/reference
   content/license
