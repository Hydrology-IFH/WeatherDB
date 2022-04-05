# libraries
from datetime import datetime, timedelta
from pathlib import Path
import warnings
import zipfile
import logging

import numpy as np
import pandas as pd

import rasterio as rio
import rasterio.mask
from shapely.geometry import Point, MultiLineString

from ..lib.connections import DB_ENG, check_superuser
from ..lib.max_fun.import_DWD import dwd_id_to_str
from ..lib.utils import TimestampPeriod
from ..lib.max_fun.geometry import polar_line, raster2points
from . import base

# variables
log = logging.getLogger(__name__)
RICHTER_CLASSES = {
    "no-protection": {
        "min_horizon": 0,
        "max_horizon": 3
    },
    "little-protection": {
        "min_horizon": 3,
        "max_horizon": 7
    },
    "protected": {
        "min_horizon": 7,
        "max_horizon": 12
    },
    "heavy-protection": {
        "min_horizon": 12,
        "max_horizon": np.inf
    },
}

# the different Station kinds:
class StationN(base.StationNBase):
    """A class to work with and get the 10 minutes precipitation data."""    
    _ftp_folder_base = [
        "climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/"]
    _para = "n"
    _para_long = "Precipitation"
    _cdc_col_names_imp = ["RWS_10", "QN"]
    _db_col_names_imp = ["raw", "qn"]
    _tstp_format = "%Y%m%d %H:%M"
    _tstp_dtype = "timestamp"
    _interval = "10 min"
    _min_agg_to = "10 min"
    _unit = "mm/10min"
    _valid_kinds = ["raw", "qn", "qc", "corr", "filled", "filled_by"]
    _best_kind = "corr"

    def __init__(self, id):
        super().__init__(id)
        self.id_str = dwd_id_to_str(id)

    def _get_sql_new_qc(self, period):
        # create sql_format_dict
        sql_format_dict = dict(
            para=self._para, stid=self.id, para_long=self._para_long,
            **period.get_sql_format_dict(
                format="'{}'".format(self._tstp_format)),
            limit=0.1*self._decimals) # don't delete values below 0.1mm/10min if they are consecutive

        # check if daily station is available
        sql_check_d = """
            SELECT EXISTS(
                SELECT *
                FROM information_schema.tables
                WHERE table_schema = 'timeseries'
                    AND table_name = '{stid}_{para}_d'
            );""".format(**sql_format_dict)
        with DB_ENG.connect() as con:
            daily_exists = con.execute(sql_check_d).first()[0]

        # create sql for dates where the aggregated 10 minutes measurements are 0
        # althought the daily measurements are not 0
        if daily_exists:
            sql_dates_failed = """
                WITH ts_10min_d AS (
                    SELECT (ts.timestamp - INTERVAL '5h 50 min')::date as date, sum("raw") as raw
                    FROM timeseries."{stid}_{para}" ts
                    WHERE ts.timestamp BETWEEN {min_tstp} AND {max_tstp}
                    GROUP BY (ts.timestamp - INTERVAL '5h 50 min')::date)
                SELECT date
                FROM timeseries."{stid}_{para}_d" ts_d
                LEFT JOIN ts_10min_d ON ts_d.timestamp::date=ts_10min_d.date
                WHERE ts_d.timestamp BETWEEN {min_tstp}::date AND {max_tstp}::date
                    AND ts_10min_d.raw = 0 AND ts_d.raw <> 0
            """.format(**sql_format_dict)
        else:
            log.warn((
                "For the {para_long} station with ID {stid} there is no timeserie with daily values. " +
                "Therefor the quality check for daily values equal to 0 is not done.\n" +
                "Please consider updating the daily stations with:\n" +
                "stats = stations.PrecipitationDailyStations()\n" +
                "stats.update_meta()\nstats.update_raw()"
            ).format(**sql_format_dict))
            sql_dates_failed = """
                SELECT NULL::date as date
            """

        # make sql for timestamps where 3 times same value in a row
        sql_tstps_failed = """
            WITH tstps_df as (
                SELECT  ts.timestamp as tstp_1,
                        ts2.timestamp as tstp_2,
                        ts3.timestamp as tstp_3
                from timeseries."{stid}_{para}" ts
                INNER JOIN timeseries."{stid}_{para}" ts2
                    on ts.timestamp = ts2.timestamp - INTERVAL '10 min'
                INNER JOIN timeseries."{stid}_{para}" ts3
                    on ts.timestamp = ts3.timestamp - INTERVAL '20 min'
                WHERE ts.qn != 3
                    AND ts.raw = ts2.raw AND ts2.raw = ts3.raw
                    AND ts.raw > {limit:n}
                    AND ts.raw is not NULL
                    AND ts.timestamp BETWEEN {min_tstp} AND {max_tstp}
            )
            SELECT tstp_1 AS timestamp FROM tstps_df
            UNION SELECT tstp_2 FROM tstps_df
            UNION SELECT tstp_3 FROM tstps_df
        """.format(**sql_format_dict)

        sql_new_qc = """
            WITH tstps_failed as ({sql_tstps_failed}),
                    dates_failed AS ({sql_dates_failed})
            SELECT ts.timestamp,
                (CASE WHEN ((ts.timestamp IN (SELECT timestamp FROM tstps_failed))
                        OR ((ts.timestamp - INTERVAL '5h 50 min')::date IN (
                            SELECT date FROM dates_failed))
                        OR ts."raw" < 0)
                    THEN NULL
                    ELSE ts."raw" END) as qc
            FROM timeseries."{stid}_{para}" ts
            WHERE ts.timestamp BETWEEN {min_tstp} AND {max_tstp}
        """.format(
            sql_tstps_failed=sql_tstps_failed,
            sql_dates_failed=sql_dates_failed,
            **sql_format_dict)

        return sql_new_qc

    def _check_df_raw(self, df_raw):
        """This function applies extra checkups on the downloaded raw timeserie and returns the dataframe.
        
        Some precipitation stations on the DWD CDC server have also rows outside of the normal 10 Minute frequency, e.g. 2008-09-16 01:47 for Station 662.
        Because those rows only have NAs for the measurement they are deleted."""
        df_raw = df_raw[df_raw.index.minute%10==0].copy()
        return df_raw

    @check_superuser
    def _create_timeseries_table(self):
        """Create the timeseries table in the DB if it is not yet existing."""
        sql_add_table = '''
            CREATE TABLE IF NOT EXISTS timeseries."{stid}_{para}"  (
                timestamp timestamp PRIMARY KEY,
                raw int4,
                qn smallint,
                qc  int4,
                filled int4,
                filled_by int2,
                corr int4
            );
        '''.format(stid=self.id, para=self._para)
        with DB_ENG.connect() as con:
            con.execute(sql_add_table)

    @staticmethod
    def _richter_class_from_horizon(horizon):
        richter_class = None
        for key in RICHTER_CLASSES:
            if (horizon >= RICHTER_CLASSES[key]["min_horizon"]) and \
                (horizon < RICHTER_CLASSES[key]["max_horizon"]):
                richter_class = key
        return richter_class

    @check_superuser
    def update_horizon(self, skip_if_exist=True):
        """Update the horizon angle (Horizontabschirmung) in the meta table.

        Get new values from the raster and put in the table.

        Parameters
        ----------
        skip_if_exist : bool, optional
            Skip updating the value if there is already a value in the meta table.
            The default is True.

        Returns
        -------
        float
            The horizon angle in degrees (Horizontabschirmung).
        """
        if skip_if_exist:
            horizon = self.get_horizon()
            if horizon is not None:
                return horizon

        # check if files are available
        for dgm_name in ["dgm5", "dgm80"]:
            if not base.RASTERS["local"][dgm_name].is_file():
                raise ValueError(
                    "The {dgm_name} was not found in the data directory under: \n{fp}".format(
                        dgm_name=dgm_name,
                        fp=str(base.RASTERS["local"][dgm_name])
                    )
                )

        # get the horizontabschirmung value
        radius = 75000 # this value got defined because the maximum height is around 4000m for germany
        with rio.open(base.RASTERS["local"]["dgm5"]) as dgm5,\
             rio.open(base.RASTERS["local"]["dgm80"]) as dgm80:
                geom = self.get_geom_shp(crs="utm")
                xy = [geom.x, geom.y]
                # sample station heght
                stat_h = list(dgm5.sample(
                    xy=[xy],
                    indexes=1,
                    masked=True))[0]
                if stat_h.mask[0]:
                    log.error("update_horizon(): No height was found for {para_long} Station {stid}. Therefor the horizon angle could not get updated.".format(
                        stid=self.id, para_long=self._para_long))
                    return None
                else:
                    stat_h = stat_h[0]

                # sample dgm for horizontabschirmung (hab)
                hab = pd.Series(
                    index=pd.Index([], name="angle", dtype=int),
                    name="horizontabschirmung")
                for angle in range(90, 271, 3):
                    dgm5_mask = polar_line(xy, radius, angle)
                    dgm5_np, dgm5_tr = rasterio.mask.mask(
                        dgm5, [dgm5_mask], crop=True)
                    dgm5_np[dgm5_np==dgm5.profile["nodata"]] = np.nan
                    dgm_gpd = raster2points(dgm5_np, dgm5_tr, crs=dgm5.crs)
                    dgm_gpd["dist"] = dgm_gpd.distance(Point(xy))

                    # check if parts are missing and fill
                    #####################################
                    dgm_gpd = dgm_gpd.sort_values("dist").reset_index(drop=True)
                    line_parts = pd.DataFrame(
                        columns=["Start_point", "radius", "line"])
                    # look for holes inside the line
                    for j in dgm_gpd[dgm_gpd["dist"].diff() > 10].index:
                        line_parts = line_parts.append(
                            {"Start_point": dgm_gpd.loc[j-1, "geometry"],
                             "radius": dgm_gpd.loc[j, "dist"] - dgm_gpd.loc[j-1, "dist"]},
                            ignore_index=True)

                    # look for missing values at the end
                    dgm5_max_dist = dgm_gpd.iloc[-1]["dist"]
                    if dgm5_max_dist < (radius - 5):
                            line_parts = line_parts.append(
                                {"Start_point": dgm_gpd.iloc[-1]["geometry"],
                                 "radius": radius - dgm5_max_dist},
                                ignore_index=True)

                    # check if parts are missing and fill
                    if len(line_parts) > 0:
                            # create the lines
                            for i, row in line_parts.iterrows():
                                line_parts.loc[i, "line"] = polar_line(
                                    [el[0] for el in row["Start_point"].xy],
                                    row["radius"],
                                    angle
                                )
                            dgm80_mask = MultiLineString(
                                line_parts["line"].tolist())
                            dgm80_np, dgm80_tr = rasterio.mask.mask(
                                dgm80, [dgm80_mask], crop=True)
                            dgm80_np[dgm80_np==dgm80.profile["nodata"]] = np.nan
                            dgm80_gpd = raster2points(
                                dgm80_np, dgm80_tr, crs=dgm80.crs
                                ).to_crs(dgm5.crs)
                            dgm80_gpd["dist"] = dgm80_gpd.distance(Point(xy))
                            dgm_gpd = dgm_gpd.append(
                                dgm80_gpd, ignore_index=True)

                    hab[angle] = np.max(np.degrees(np.arctan(
                            (dgm_gpd["data"]-stat_h) / dgm_gpd["dist"])))

                # calculate the mean "horizontabschimung"
                # Richter: H’=0,15H(S-SW) +0,35H(SW-W) +0,35H(W-NW) +0, 15H(NW-N)
                horizon = max(0,
                    0.15*hab[(hab.index>225) & (hab.index<=270)].mean()
                    + 0.35*hab[(hab.index>=180) & (hab.index<=225)].mean()
                    + 0.35*hab[(hab.index>=135) & (hab.index<180)].mean()
                    + 0.15*hab[(hab.index>=90) & (hab.index<135)].mean())

                # insert to meta table in db
                self._update_meta(
                    cols=["horizontabschirmung"],
                    values=[horizon])

                return horizon

    @check_superuser
    def update_richter_class(self, skip_if_exist=True):
        """Update the richter class in the meta table.

        Get new values from the raster and put in the table.

        Parameters
        ----------
        skip_if_exist : bool, optional
            Skip updating the value if there is already a value in the meta table.
            The default is True

        Returns
        -------
        str
            The richter class name.
        """
        # check if already value in table
        if skip_if_exist:
            richter_class = self.get_richter_class()
            if self.get_richter_class() is not None:
                return richter_class

        # get the richter class
        richter_class = self._richter_class_from_horizon(
            horizon=self.update_horizon(skip_if_exist=skip_if_exist))

        # save to db
        self._update_meta(
                cols=["richter_class"],
                values=[richter_class])

        return richter_class

    @check_superuser
    def richter_correct(self, period=(None, None), **kwargs):
        """Do the richter correction on the filled data for the given period.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).

        Raises
        ------
        Exception
            If no richter class was found for this station.
        """
        # check if period is given
        if type(period) != TimestampPeriod:
            period = TimestampPeriod(*period)
        period_in = period.copy()
        if not period.is_empty():
            period = self._check_period(
                period=period, kinds=["filled"])
            sql_period_clause = """
                WHERE timestamp BETWEEN {min_tstp} AND {max_tstp}
            """.format(
                **period.get_sql_format_dict(
                    format="'{}'".format(self._tstp_format)
                )
            )
        else:
            sql_period_clause = ""

        # check if temperature station is filled
        stat_t = StationT(self.id)
        stat_t_period = stat_t.get_filled_period(kind="filled")
        stat_n_period = self.get_filled_period(kind="filled")
        delta = timedelta(hours=5, minutes=50)
        min_date = pd.Timestamp(base.MIN_TSTP).date()
        stat_t_min = stat_t_period[0].date()
        stat_t_max = stat_t_period[1].date()
        stat_n_min = (stat_n_period[0] - delta).date()
        stat_n_max = (stat_n_period[1] - delta).date()
        if stat_t_period.is_empty()\
                or (stat_t_min > stat_n_min
                    and not (stat_n_min < min_date)
                            and (stat_t_min == min_date)) \
                or (stat_t_max  < stat_n_max)\
                and not stat_t.is_last_imp_done(kind="filled"):
            stat_t.fillup(period=period)

        # get the richter exposition class
        richter_class = self.update_richter_class(skip_if_exist=True)
        if richter_class is None:
            raise Exception("No richter class was found for the precipitation station {stid} and therefor no richter correction was possible."
                            .format(stid=self.id))

        # create the sql queries
        sql_format_dict = dict(
            stid=self.id,
            para=self._para,
            richter_class=richter_class,
            period_clause=sql_period_clause,
            n_decim=self._decimals,
            t_decim=stat_t._decimals
        )
        # daily precipitation
        sql_n_daily = """
            SELECT (timestamp - INTERVAL '5h 50min')::date AS date,
                   sum("filled") AS "filled",
                   count(*) FILTER (WHERE "filled" > 0) AS "count_n"
            FROM timeseries."{stid}_{para}"
            {period_clause}
            GROUP BY (timestamp - INTERVAL '5h 50min')::date
        """.format(**sql_format_dict)

        # add is_winter
        sql_n_daily_winter = """
            SELECT date,
			    CASE WHEN EXTRACT(MONTH FROM date) IN (1, 2, 3, 10, 11, 12)
			            THEN true::bool
			            ELSE false::bool
			    END AS is_winter,
			    "filled" AS "n_d",
			    "count_n"
			FROM ({sql_n_daily}) tsn_d
        """.format(sql_n_daily=sql_n_daily)

        # add precipitation class
        sql_n_daily_precip_class = """
            SELECT
                date, "count_n", "n_d",
                CASE WHEN (tst."filled" >= (3 * {t_decim}) AND "is_winter") THEN 'precip_winter'
                    WHEN (tst."filled" >= (3 * {t_decim}) AND NOT "is_winter") THEN 'precip_summer'
                    WHEN (tst."filled" <= (-0.7 * {t_decim})::int) THEN 'snow'
                    WHEN (tst."filled" IS NULL) THEN NULL
                    ELSE 'mix'
                END AS precipitation_typ
            FROM ({sql_n_daily_winter}) tsn_d
            LEFT JOIN timeseries."{stid}_t" tst
                ON tst.timestamp=tsn_d.date
        """.format(
            sql_n_daily_winter=sql_n_daily_winter,
            **sql_format_dict
        )

        # calculate the delta n
        sql_delta_n = """
            SELECT date,
                CASE WHEN "count_n"> 0 THEN
                        round(("b_{richter_class}" * ("n_d"::float/{n_decim})^"E" * {n_decim})/"count_n")
                    ELSE 0
                END AS "delta_10min"
            FROM ({sql_n_daily_precip_class}) tsn_d2
            LEFT JOIN richter_values r
                ON r."precipitation_typ"=tsn_d2."precipitation_typ"
        """.format(
            sql_n_daily_precip_class=sql_n_daily_precip_class,
            **sql_format_dict
        )

        # calculate the new corr
        sql_update = """
            UPDATE timeseries."{stid}_{para}" ts
            SET "corr" = CASE WHEN "filled" > 0
                            THEN ts."filled" + ts_delta_n."delta_10min"
                            ELSE ts."filled"
                            END
            FROM ({sql_delta_n}) ts_delta_n
            WHERE (ts.timestamp - INTERVAL '5h 50min')::date = ts_delta_n.date;
        """.format(
            sql_delta_n=sql_delta_n,
            **sql_format_dict
        )

        # run commands
        if "return_sql" in kwargs and kwargs["return_sql"]:
            return sql_update.replace("%%", "%")
        self._execute_long_sql(
            sql_update,
            description="richter corrected for the period {min_tstp} - {max_tstp}".format(
                **period.get_sql_format_dict(format="%Y-%m-%d %H:%M")
            ))

        # mark last import as done, if previous are ok
        if (self.is_last_imp_done(kind="qc") and self.is_last_imp_done(kind="filled")):
            if (period_in.is_empty() or
                period_in.contains(self.get_last_imp_period())):
                self._mark_last_imp_done(kind="corr")

        # calculate the difference to filled timeserie
        if period.is_empty() or period[0].year < pd.Timestamp.now().year:
            sql_diff_filled = """
                UPDATE meta_n
                SET quot_corr_filled = quot_avg
                FROM (
                    SELECT avg(quot)*100 AS quot_avg
                    FROM (
                        SELECT sum("corr")::float/sum("filled")::float AS quot
                        FROM timeseries."{stid}_{para}"
                        GROUP BY date_trunc('year', "timestamp")
                        HAVING count("filled") > 364 * 6 *24) df_y) df_avg
                WHERE station_id={stid};""".format(**sql_format_dict)

            with DB_ENG.connect() as con:
                con.execute(sql_diff_filled)

        # update filled time in meta table
        self.update_period_meta(kind="corr")

    @check_superuser
    def corr(self, period=(None, None)):
        self.richter_correct(period=period)

    @check_superuser
    def last_imp_richter_correct(self, _last_imp_period=None):
        """Do the richter correction of the last import.

        Parameters
        ----------
        _last_imp_period : _type_, optional
            Give the overall period of the last import.
            This is only for intern use of the stationsN methode to not compute over and over again the period.
            The default is None.
        """
        if not self.is_last_imp_done(kind="corr"):
            if _last_imp_period is None:
                period = self.get_last_imp_period(all=True)
            else:
                period = _last_imp_period

            self.richter_correct(
                period=period)

            if self.is_last_imp_done(kind="qc") \
                    and self.is_last_imp_done(kind="filled"):
                self._mark_last_imp_done(kind="corr")
        else:
            log.info("The last import of {para_long} Station {stid} was already richter corrected and is therefor skiped".format(
                stid=self.id, para_long=self._para_long
            ))

    @check_superuser
    def last_imp_corr(self, _last_imp_period=None):
        """A wrapper for last_imp_richter_correct()."""
        return self.last_imp_richter_correct(_last_imp_period=_last_imp_period)

    @check_superuser
    def _sql_extra_fillup(self):
        sql_extra = """
            UPDATE new_filled_{stid}_{para} ts
            SET filled = filled * coef
            FROM (
                SELECT
                    date,
                    ts_d."raw"/ts_10."filled"::float AS coef
                FROM (
                    SELECT
                        date(timestamp - '5h 50min'::INTERVAL),
                        sum(filled) AS filled
                    FROM new_filled_{stid}_{para}
                    GROUP BY date(timestamp - '5h 50min'::INTERVAL)
                    ) ts_10
                LEFT JOIN timeseries."{stid}_n_d" ts_d
                    ON ts_10.date=ts_d.timestamp
                WHERE ts_d."raw" IS NOT NULL
                      AND ts_10.filled > 0
                ) df_coef
            WHERE (ts.timestamp - '5h 50min'::INTERVAL)::date = df_coef.date
                AND coef != 1;
        """.format(stid=self.id, para=self._para)

        return sql_extra

    @check_superuser
    def fillup(self, period=(None, None), **kwargs):
        super_ret = super().fillup(period=period, **kwargs)

        # check the period
        if type(period) != TimestampPeriod:
            period= TimestampPeriod(*period)

        # update difference to regnie
        if period.is_empty() or period[0].year < pd.Timestamp.now().year:
            sql_diff_ma = """
                UPDATE meta_n
                SET quot_filled_regnie = quots.quot_regnie,
                    quot_filled_dwd_grid = quots.quot_dwd
                FROM (
                    SELECT df_ma.ys / (srv.n_regnie_year*100) AS quot_regnie,
                        df_ma.ys / (srv.n_year*100) AS quot_dwd
                    FROM (
                        SELECT avg(df_a.yearly_sum) as ys
                        FROM (
                            SELECT sum("filled") AS yearly_sum
                            FROM timeseries."{stid}_{para}"
                            GROUP BY date_trunc('year', "timestamp")
                            HAVING count("filled") > 363 * 6 * 24) df_a
                        ) df_ma
                    LEFT JOIN stations_raster_values srv
                        ON station_id={stid}) quots
                WHERE station_id ={stid};
            """.format(stid=self.id, para=self._para)

            #execute sql or return
            if "return_sql" in kwargs and kwargs["return_sql"]:
                return (str(super_ret) + "\n" + sql_diff_ma).replace("%%", "%")
            with DB_ENG.connect() as con:
                con.execute(sql_diff_ma)

    def get_corr(self, period=(None, None)):
        return self.get_df(period=period, kinds=["corr"])

    def get_qn(self, period=(None, None)):
        return self.get_df(period=period, kinds=["qn"])

    def get_richter_class(self, update_if_fails=True):
        """Get the richter class for this station.

        Provide the data from the meta table.

        Parameters
        ----------
        update_if_fails: bool, optional
            Should the richter class get updatet if no exposition class is found in the meta table?
            If False and no exposition class was found None is returned.
            The default is True.

        Returns
        -------
        string
            The corresponding richter exposition class.
        """
        sql = """
            SELECT richter_class
            FROM meta_{para}
            WHERE station_id = {stid}
        """.format(stid=self.id, para=self._para)

        with DB_ENG.connect() as con:
            res = con.execute(sql).first()

        # check result
        if res is None:
            if update_if_fails:
                self.update_richter_class()
                # update_if_fails is False to not get an endless loop
                return self.get_richter_class(update_if_fails=False)
            else:
                return None
        else:
            return res[0]

    def get_horizon(self):
        """Get the value for the horizon angle. (Horizontabschirmung)

        This value is defined by Richter (1995) as the mean horizon angle in the west direction as:
        H’=0,15H(S-SW) +0,35H(SW-W) +0,35H(W-NW) +0, 15H(NW-N)

        Returns
        -------
        float or None
            The mean western horizon angle
        """
        return self.get_meta(infos="horizontabschirmung")


class StationND(base.StationNBase, base.StationCanVirtualBase):
    """A class to work with and get the daily precipitation data.
    Those timeseries are actually only downloaded to make a quality check on the 10 minutes data. 
    To download data (also daily) please use the StationN to get 10 minute values and aggregate them. (or use "agg_to"-parameter to get them aggregated.)
    """  
    _ftp_folder_base = [
        "climate_environment/CDC/observations_germany/climate/daily/kl/",
        "climate_environment/CDC/observations_germany/climate/daily/more_precip/"]
    _para = "n_d"
    _para_long = "daily Precipitation"
    _cdc_col_names_imp = ["RSK"]
    _db_col_names_imp = ["raw"]
    _tstp_format = "%Y%m%d"
    _tstp_dtype = "date"
    _interval = "1 day"
    _min_agg_to = "day"
    _unit = "mm/day"
    _valid_kinds = ["raw", "filled", "filled_by"]
    _best_kind = "filled"

    # methods from the base class that should not be active for this class
    quality_check = property(doc='(!) Disallowed inherited')
    last_imp_quality_check = property(doc='(!) Disallowed inherited')
    get_corr = property(doc='(!) Disallowed inherited')
    get_adj = property(doc='(!) Disallowed inherited')
    get_qc = property(doc='(!) Disallowed inherited')

    def __init__(self, id):
        super().__init__(id)
        self.id_str = dwd_id_to_str(id)

    def _download_raw(self, zipfiles):
        df_all = super()._download_raw(zipfiles)

        # fill RSK with values from RS if not given
        if "RS" in df_all.columns and "RSK" in df_all.columns:
            mask = df_all["RSK"].isna()
            df_all.loc[mask, "RSK"] = df_all.loc[mask, "RS"]
        elif "RS" in df_all.columns:
            df_all["RSK"] = df_all["RS"]

        return df_all

    @check_superuser
    def _create_timeseries_table(self):
        """Create the timeseries table in the DB if it is not yet existing."""
        sql_add_table = '''
            CREATE TABLE IF NOT EXISTS timeseries."{stid}_{para}"  (
                timestamp date PRIMARY KEY,
                raw int4,
                filled int4,
                filled_by int2
            );
        '''.format(stid=self.id, para=self._para)
        with DB_ENG.connect() as con:
            con.execute(sql_add_table)


class StationT(base.StationTETBase):
    """A class to work with and get the temperature data."""  
    _ftp_folder_base = [
        "climate_environment/CDC/observations_germany/climate/daily/kl/"]
    _date_col = "MESS_DATUM"
    _para = "t"
    _para_long = "Temperature"
    _cdc_col_names_imp = ["TMK"]
    _unit = "°C"
    _decimals = 10
    _ma_cols = ["t_year"]
    _coef_sign = ["-", "+"]
    _agg_fun = "avg"

    def __init__(self, id):
        super().__init__(id)
        self.id_str = dwd_id_to_str(id)

    def _get_sql_new_qc(self, period):
        # create sql for new qc
        sql_new_qc = """
            WITH nears AS ({sql_near_mean})
            SELECT
                timestamp,
                (CASE WHEN (ABS(nears.raw - nears.mean) > 5)
                    THEN NULL
                    ELSE nears."raw" END) as qc
            FROM nears
        """ .format(
            sql_near_mean=self._get_sql_near_mean(period=period, only_real=True))

        return sql_new_qc

    def get_multi_annual(self):
        mas = super().get_multi_annual()
        if mas is not None:
            return [ma / 10 for ma in mas]
        else:
            return None

    def get_adj(self, period=(None, None)):
        main_df, adj_df, ma, main_df_tr = super().get_adj(period=period)

        # calculate the yearly
        main_df_y = main_df.groupby(main_df_tr.index.year)\
            .mean().mean()

        adj_df["adj"] = (main_df + (ma[0] - main_df_y)).round(1)

        return adj_df


class StationET(base.StationTETBase):
    """A class to work with the potential Evapotranspiration data. (VPGB)"""  
    _ftp_folder_base = ["climate_environment/CDC/derived_germany/soil/daily/"]
    _date_col = "Datum"
    _para = "et"
    _para_long = "Evapotranspiration"
    _cdc_col_names_imp = ["VPGB"]
    _unit = "mm/Tag"
    _decimals = 10
    _ma_cols = ["et_year"]
    _sql_add_coef_calc = "* ma.exp_fact::float/ma_stat.exp_fact::float"

    def __init__(self, id):
        super().__init__(id)
        self.id_str = str(id)

    def _get_sql_new_qc(self, period):
        # create sql for new qc
        sql_new_qc = """
            WITH nears AS ({sql_near_mean})
            SELECT
                timestamp,
                (CASE WHEN ((nears.raw > (nears.mean * 2) AND nears.raw > 3)
                            OR (nears.raw < (nears.mean * 0.5) AND nears.raw > 2))
                    THEN NULL
                    ELSE nears."raw" END) as qc
            FROM nears
        """ .format(
            sql_near_mean=self._get_sql_near_mean(
                period=period, only_real=True)
        )

        return sql_new_qc

    def get_adj(self, period=(None, None)):
        main_df, adj_df, ma, main_df_tr = super().get_adj(period=period)

        # calculate the yearly
        main_df_y = main_df.groupby(main_df_tr.index.year)\
            .sum(min_count=345).mean()

        adj_df["adj"] = (main_df * (ma[0] / main_df_y)).round(1)

        return adj_df


# create a grouping class for the 3 parameters together
class GroupStation(object):
    """A class to group all possible parameters of one station.

    So if you want to create the input files for a simulation, where you neet T, ET and N, use this class to download the data.
    """

    def __init__(self, id, error_if_missing=True):
        self.id = id
        self.station_parts = []
        for StatClass in [StationN, StationT, StationET]:
            try:
                self.station_parts.append(
                    StatClass(id=id)
                )
            except Exception as e:
                if error_if_missing:
                    raise e

    def get_possible_paras(self, short=False):
        """Get the possible parameters for this station.

        Parameters
        ----------
        short : bool, optional
            Should the short name of the parameters be returned.
            The default is "long".

        Returns
        -------
        list of str
            A list of the long parameter names that are possible for this station to get.
        """
        paras = []
        attr_name = "_para" if short else "_para_long"
        for stat in self.station_parts:
            paras.append(getattr(stat, attr_name))

        return paras

    def get_filled_period(self, kind="best", from_meta=True):
        """Get the combined filled period for all 3 stations.

        This is the maximum possible timerange for these stations.

        Parameters
        ----------
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        from_meta : bool, optional
            Should the period be from the meta table?
            If False: the period is returned from the timeserie. In this case this function is only a wrapper for .get_period_meta.
            The default is True.

        Returns
        -------
        TimestampPeriod
            The maximum filled period for the 3 parameters for this station.
        """
        filled_period = self.station_parts[0].get_filled_period(
            kind=kind, from_meta=from_meta)
        for stat in self.station_parts[1:]:
            filled_period = filled_period.union(
                stat.get_filled_period(kind=kind, from_meta=from_meta),
                how="inner")
        return filled_period

    def get_df(self, period=(None, None), kind="best", paras="all", agg_to="10 min"):
        """Get a DataFrame with the corresponding data.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        agg_to : str, optional
            To what aggregation level should the timeseries get aggregated to.
            The minimum aggregation for Temperatur and ET is daily and for the precipitation it is 10 minutes.
            If a smaller aggregation is selected the minimum possible aggregation for the respective parameter is returned.
            So if 10 minutes is selected, than precipitation is returned in 10 minuets and T and ET as daily.
            The default is "10 min".

        Returns
        -------
        pd.Dataframe
            A DataFrame with the timeseries for this station and the given period.
        """
        dfs = []
        for stat in self.station_parts:
            if paras == "all" or stat._para in paras:
                df = stat.get_df(
                    period=period, kinds=[kind], agg_to=agg_to)
                df = df.rename(dict(zip(
                    df.columns,
                    [stat._para + "_" + kind])),
                    axis=1)
                dfs.append(df)

        # concat the dfs
        if len(dfs) > 1:
            df_all = pd.concat(dfs, axis=1)
        elif len(dfs) == 1 :
            df_all = dfs[0]
        else:
            raise ValueError("No timeserie was found for {paras} and Station {stid}".format(
                paras=", ".join(paras) is type(paras),
                stid=self.id))

        return df_all

    def get_geom(self):
        return self.station_parts[0].get_geom()

    def get_name(self):
        return self.station_parts[0].get_name()

    def create_roger_ts(self, dir, period=(None, None),
                        kind="best", et_et0=1):
        """Create the timeserie files for roger as csv.

        This is only a wrapper function for create_ts with some standard settings.

        Parameters
        ----------
        dir : pathlib like object or zipfile.ZipFile
            The directory or Zipfile to store the timeseries in.
            If a zipfile is given a folder with the statiopns ID is added to the filepath.
        period : TimestampPeriod like object, optional
            The period for which to get the timeseries.
            If (None, None) is entered, then the maximal possible period is computed.
            The default is (None, None)
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        et_et0: int or None, optional

        Raises
        ------
        Warning
            If there are NAs in the timeseries or the period got changed.
        """
        return self.create_ts(dir=dir, period=period, kind=kind,
                              agg_to="10 min", et_et0=et_et0, split_date=True)

    def create_ts(self, dir, period=(None, None), kind="best",
                  agg_to="10 min", et_et0=None, split_date=False):
        """Create the timeserie files as csv.

        Parameters
        ----------
        dir : pathlib like object or zipfile.ZipFile
            The directory or Zipfile to store the timeseries in.
            If a zipfile is given a folder with the statiopns ID is added to the filepath.
        period : TimestampPeriod like object, optional
            The period for which to get the timeseries.
            If (None, None) is entered, then the maximal possible period is computed.
            The default is (None, None)
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        agg_to : str, optional
            To what aggregation level should the timeseries get aggregated to.
            The minimum aggregation for Temperatur and ET is daily and for the precipitation it is 10 minutes.
            If a smaller aggregation is selected the minimum possible aggregation for the respective parameter is returned.
            So if 10 minutes is selected, than precipitation is returned in 10 minuets and T and ET as daily.
            The default is "10 min".
        et_et0 : int or None, optional
            Should the ET timeserie contain a column with et_et0.
            If None, then no column is added.
            If int, then a ET/ET0 column is appended with this number as standard value.
            Until now providing a serie of different values is not possible.
            The default is None.
        split_date : bool, optional
            Should the timestamp get splitted into parts, so one column for year, one for month etc.?
            If False the timestamp is saved in one column as string.

        Raises
        ------
        Warning
            If there are NAs in the timeseries or the period got changed.
        """
        # check directory
        dir = self._check_dir(dir)

        # get the period
        period = TimestampPeriod._check_period(period)
        period_filled = self.get_filled_period(kind=kind)

        if period.is_empty():
            period = period_filled
        else:
            period_new = period_filled.union(
                period,
                how="inner")
            if period_new != period:
                warnings.warn(
                    "The Period for Station {stid} got changed from {period} to {period_filled}.".format(
                        stid=self.id,
                        period=str(period),
                        period_filled=str(period_filled)))
                period = period_new

        # prepare loop
        name_suffix = "_{stid:0>4}.txt".format(stid=self.id)
        x, y = self.get_geom().split(";")[1]\
                .replace("POINT(", "").replace(")", "")\
                .split(" ")
        name = self.get_name() + " (ID: {stid})".format(stid=self.id)
        do_zip = type(dir) == zipfile.ZipFile

        for para in ["n", "t", "et"]:
            # get the timeserie
            df = self.get_df(
                period=period, kind=kind,
                paras=[para], agg_to=agg_to)

            # rename columns
            df.rename(dict(zip(df.columns, para.upper())), axis=1, inplace=True)

             # check for NAs
            if df.isna().sum().sum() > 0 and kind in ["filled", "corr", "best"]:
                warnings.warn("There were NAs in the timeserie for Station {stid}. this should not happen. Please review the code and the database.".format(
                    stid=self.id))

            # get the number of columns
            num_col = 1
            if split_date:
                num_col += base.AGG_TO[agg_to]["split"][para]
            else:
                num_col += 1

            # special operations for et
            if para == "et" and et_et0 is None:
                num_col += 1
                df = df.join(
                    pd.Series([et_et0]*len(df), name="R/R0", index=df.index))

            # create header
            header = ("Name: " + name + "\t" * (num_col-1) + "\n" +
                        "Lat: " + y + "   ,Lon: " + x + "\t" * (num_col-1) + "\n")

            # create tables
            if split_date:
                df = self._split_date(df.index)\
                        .iloc[:, 0:base.AGG_TO[agg_to]["split"][para]]\
                        .join(df)
            else:
                df.reset_index(inplace=True)

            # write table out
            str_df = header + df.to_csv(sep="\t", decimal=".", index=False)
            file_name = para.upper() + name_suffix
            if do_zip:
                dir.writestr(
                    "{stid}/{file}".format(
                        stid=self.id, file=file_name),
                    str_df)
            else:
                with open(dir.joinpath(file_name), "w") as f:
                    f.write(str_df)

    @staticmethod
    def _check_dir(dir):
        """Checks if a directors is valid and empty.

        If not existing the directory is created.

        Parameters
        ----------
        dir : pathlib object or zipfile.ZipFile
            The directory to check.

        Raises
        ------
        ValueError
            If the directory is not empty.
        ValueError
            If the directory is not valid. E.G. it is a file path.
        """
        # check types
        if type(dir) == str:
            dir = Path(dir)

        # check directory
        if isinstance(dir, Path):
            if dir.is_dir():
                if len(list(dir.iterdir())) > 0:
                    raise ValueError(
                        "The given directory '{dir}' is not empty.".format(
                            dir=str(dir)))
            elif dir.suffix == "":
                dir.mkdir()
            else:
                raise ValueError(
                    "The given directory '{dir}' is not a directory.".format(
                        dir=dir))
        elif not isinstance(dir, zipfile.ZipFile):
            raise ValueError(
                "The given directory '{dir}' is not a directory or zipfile.".format(
                    dir=dir))

        return dir

    @staticmethod
    def _split_date(dates):
        """
        Split datetime into parts.

        Parameters
        ----------
        dates : pandas.DatetimeIndex or list of (datetime.dateime or pandas.Timestamp) or
                pandas.DataFrame of (datetime.datetime or pandas.Timestamp)
            The datetime's to split.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with 5 columns (Jahr, Monat, Tag, Stunde, Minute).
        """
        # if dates is not a list make it a list
        if type(dates) == datetime or type(dates) == pd.Timestamp:
            dates = pd.DatetimeIndex([dates])
            index = range(0, len(dates))

        elif type(dates) == pd.DatetimeIndex:
            index = dates
        else:
            index = range(0, len(dates))

        # check if date is datetime or Timestamp:
        if not (type(dates[0]) == pd.Timestamp or
                type(dates[0]) == datetime):
            raise ValueError("Error: The given date is not in a datetime or " +
                            "Timestamp format.")

        return pd.DataFrame(
            {"Jahr": dates.year,
             "Monat": dates.month,
             "Tag": dates.day,
             "Stunde": dates.hour,
             "Minute": dates.minute},
             dtype=int,
             index=index)
