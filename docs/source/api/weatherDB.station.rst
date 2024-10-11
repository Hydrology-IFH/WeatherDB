station
-------
.. module:: weatherDB.station

StationP
^^^^^^^^
.. autoclass:: weatherDB.station.StationP

  .. autoclasstoc::

StationT
^^^^^^^^
.. autoclass:: weatherDB.station.StationT

  .. autoclasstoc::

StationET
^^^^^^^^^

.. autoclass:: weatherDB.station.StationET

  .. autoclasstoc::

StationPD
^^^^^^^^^
.. autoclass:: weatherDB.station.StationPD
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

.. autoclass:: weatherDB.station.StationBases.StationBase

  .. autoclasstoc::

.. autoclass:: weatherDB.station.StationBases.StationPBase

  .. autoclasstoc::

.. autoclass:: weatherDB.station.StationBases.StationCanVirtualBase

  .. autoclasstoc::

.. autoclass:: weatherDB.station.StationBases.StationTETBase

  .. autoclasstoc::