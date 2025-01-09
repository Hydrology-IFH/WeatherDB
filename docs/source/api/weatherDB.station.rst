station
-------
.. module:: weatherdb.station

StationP
^^^^^^^^
.. autoclass:: weatherdb.station.StationP

  .. autoclasstoc::

StationT
^^^^^^^^
.. autoclass:: weatherdb.station.StationT

  .. autoclasstoc::

StationET
^^^^^^^^^

.. autoclass:: weatherdb.station.StationET

  .. autoclasstoc::

StationPD
^^^^^^^^^
.. autoclass:: weatherdb.station.StationPD
   :exclude-members: quality_check, last_imp_quality_check, get_corr, get_adj, get_qc

  .. autoclasstoc::

GroupStation
^^^^^^^^^^^^
.. autoclass:: weatherdb.station.GroupStation

   .. autoclasstoc::

StationBase...
^^^^^^^^^^^^^^
Those are the base station classes on which the real station classes above depend on.
None of them is working on its own, because the class variables are not yet set correctly.

.. autoclass:: weatherdb.station.StationBases.StationBase

  .. autoclasstoc::

.. autoclass:: weatherdb.station.StationBases.StationPBase

  .. autoclasstoc::

.. autoclass:: weatherdb.station.StationBases.StationCanVirtualBase

  .. autoclasstoc::

.. autoclass:: weatherdb.station.StationBases.StationTETBase

  .. autoclasstoc::