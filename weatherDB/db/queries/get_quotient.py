import pandas as pd
import sqlalchemy as sa
from ..views import StationKindQuotientView, StationMATimeserieRasterQuotientView


def _get_quotient(con, stids, paras, kinds_num, kinds_denom, return_as):
    """Get the quotient of multi-annual means of two different kinds or the timeserie and the multi annual raster value.

    $quotient = \overline{ts}_{kind_num} / \overline{ts}_{denom}$

    Parameters
    ----------
    con : sqlalchemy.engine.base.Connection
        The connection to the database.
    stids : list of int or int or None
        The station ids to get the quotient from.
        If None, then all stations in the database are used.
    paras : list of str or str
        The parameters to get the quotient from.
    kinds_num : list of str or str
        The timeseries kinds of the numerators.
        Should be one of ['raw', 'qc', 'filled'].
        For precipitation also "corr" is possible.
    kinds_denom : list of str or str
        The timeseries kinds of the denominator or the multi annual raster key.
        If the denominator is a multi annual raster key, then the result is the quotient of the timeserie and the raster value.
        Possible values are:
            - for timeserie kinds: 'raw', 'qc', 'filled' or for precipitation also "corr".
            - for raster keys: 'hyras', 'dwd' or 'regnie', depending on your defined raster files.
    return_as : str
        The format of the return value.
        If "df" then a pandas DataFrame is returned.
        If "json" then a list with dictionaries is returned.

    Returns
    -------
    pandas.DataFrame or list of dict
        The quotient of the two timeseries as DataFrame or list of dictionaries (JSON) depending on the return_as parameter.
        The default is pd.DataFrame.

    Raises
    ------
    ValueError
        If the input parameters were not correct.
    """
    # split ts and raster kinds
    rast_keys = {"hyras", "regnie", "dwd"}
    kinds_denom_ts = set(kinds_denom) - rast_keys
    kinds_denom_rast = set(kinds_denom) & rast_keys

    # create tests
    tests_ts = []
    tests_rast = []

    # create stids tests
    if (isinstance(stids, list) or isinstance(stids, set)) and len(stids) == 1:
        stids = stids[0]
    if isinstance(stids, int):
        tests_ts.append(StationKindQuotientView.station_id == stids)
        tests_rast.append(StationMATimeserieRasterQuotientView.station_id == stids)
    elif isinstance(stids, list) or isinstance(stids, set):
        tests_ts.append(StationKindQuotientView.station_id.in_(stids))
        tests_rast.append(StationMATimeserieRasterQuotientView.station_id.in_(stids))
    elif stids is not None:
        raise ValueError("The stids parameter should be a list of integers or an integer or None.")

    # create paras tests
    if (isinstance(paras, list) or isinstance(paras, set)) and len(paras) == 1:
        paras = paras[0]
    if isinstance(paras, str):
        tests_ts.append(StationKindQuotientView.parameter == paras)
        tests_rast.append(StationMATimeserieRasterQuotientView.parameter.like(f"{paras}%"))
    elif isinstance(paras, list) or isinstance(paras, set):
        tests_ts.append(StationKindQuotientView.parameter.in_(paras))
        ors = []
        for para in paras:
            ors.append(StationMATimeserieRasterQuotientView.parameter.like(f"{para}%"))
        tests_rast.append(sa.or_(*ors))
    else:
        raise ValueError("The paras parameter should be a list of strings or a string.")

    # create kinds_num tests
    if (isinstance(kinds_num, list) or isinstance(kinds_num, set)) and len(kinds_num) == 1:
        kinds_num = kinds_num[0]
    if isinstance(kinds_num, str):
        tests_ts.append(StationKindQuotientView.kind_numerator == kinds_num)
        tests_rast.append(StationMATimeserieRasterQuotientView.kind == kinds_num)
    elif isinstance(kinds_num, list) or isinstance(kinds_num, set):
        tests_ts.append(StationKindQuotientView.kind_numerator.in_(kinds_num))
        tests_rast.append(StationMATimeserieRasterQuotientView.kind.in_(kinds_num))
    else:
        raise ValueError("The kinds_num parameter should be a list of strings or a string.")

    # create kinds_denom tests
    if (isinstance(kinds_denom, list) or isinstance(kinds_denom, set)) and len(kinds_denom) == 1:
        kinds_denom = kinds_denom[0]
    if isinstance(kinds_denom, str):
        tests_ts.append(StationKindQuotientView.kind_denominator == kinds_denom)
        tests_rast.append(StationMATimeserieRasterQuotientView.raster_key == kinds_denom)
    elif isinstance(kinds_denom, list) or isinstance(kinds_denom, set):
        tests_ts.append(StationKindQuotientView.kind_denominator.in_(kinds_denom))
        tests_rast.append(StationMATimeserieRasterQuotientView.raster_key.in_(kinds_denom))
    else:
        raise ValueError("The kinds_denom parameter should be a list of strings or a string.")

    # get the quotient from the database views
    data = []
    # get timeseries quotient
    if len(kinds_denom_ts) > 0:
        stmnt = sa\
            .select(
                StationKindQuotientView.station_id,
                StationKindQuotientView.parameter,
                StationKindQuotientView.kind_numerator,
                StationKindQuotientView.kind_denominator,
                StationKindQuotientView.value)\
            .select_from(StationKindQuotientView.__table__)\
            .where(sa.and_(*tests_ts))

        data = con.execute(stmnt).all()

    # get raster quotient
    if len(kinds_denom_rast) > 0:
        stmnt = sa.\
            select(
                StationMATimeserieRasterQuotientView.station_id,
                StationMATimeserieRasterQuotientView.parameter,
                StationMATimeserieRasterQuotientView.kind.label("kind_numerator"),
                StationMATimeserieRasterQuotientView.raster_key.label("kind_denominator"),
                StationMATimeserieRasterQuotientView.value)\
            .select_from(
                StationMATimeserieRasterQuotientView.__table__)\
            .where(sa.and_(*tests_rast))

        data = data + con.execute(stmnt).all()

    # create return value
    if not any(data):
        data = []
    if return_as == "json":
        return [
            dict(
                station_id=row[0],
                parameter=row[1],
                kind_num=row[2],
                kind_denom=row[3],
                value=row[4])
            for row in data]
    elif return_as == "df":
        return pd.DataFrame(
            data,
            columns=["station_id", "parameter", "kind_num", "kind_denom", "value"]
        ).set_index(["station_id", "parameter", "kind_num", "kind_denom"])
    else:
        raise ValueError("The return_as parameter was not correct. Use one of ['df', 'json'].")