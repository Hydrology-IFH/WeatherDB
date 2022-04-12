station
-------
.. module:: weatherDB.station

StationN
^^^^^^^^
.. autoclass:: weatherDB.station.StationN

  .. autoclasstoc::

StationT
^^^^^^^^
.. autoclass:: weatherDB.station.StationT

  .. autoclasstoc::

StationET
^^^^^^^^^

.. autoclass:: weatherDB.station.StationET

  .. autoclasstoc::

StationND
^^^^^^^^^
.. autoclass:: weatherDB.station.StationND
   :exclude-members: quality_check, last_imp_quality_check, get_corr, get_adj, get_qc

  .. autoclasstoc::

GroupStation
^^^^^^^^^^^^
.. autoclass:: weatherDB.station.GroupStation
  
   .. autoclasstoc::

StationBase...
^^^^^^^^^^^^^^
Those are the base station classes on which the real station classes above depend on. 
None of them is working on its own, because the class variables are not yet set correctly.

.. autoclass:: weatherDB.station.StationBase

  .. autoclasstoc::

.. autoclass:: weatherDB.station.StationNBase

  .. autoclasstoc::

.. autoclass:: weatherDB.station.StationCanVirtualBase

  .. autoclasstoc::

.. autoclass:: weatherDB.station.StationTETBase

  .. autoclasstoc::