# libraries
import logging
from datetime import timedelta
from pathlib import Path
import warnings
import numpy as np
import pandas as pd
from sqlalchemy import text as sqltxt
import rasterio as rio
import rasterio.mask
from shapely.geometry import MultiLineString
import geopandas as gpd
from contextlib import nullcontext

from ..db.connections import db_engine
from ..utils.dwd import  dwd_id_to_str
from ..utils.TimestampPeriod import TimestampPeriod
from ..utils.geometry import polar_line, raster2points
from ..config import config
from ..db.models import MetaN
from .StationBases import StationNBase
from .constants import MIN_TSTP
from .StationND import StationND
from .StationT import StationT

# set settings
# ############
__all__ = ["StationN"]
log = logging.getLogger(__name__)

# variables
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

# class definition
##################
class StationN(StationNBase):
    """A class to work with and download 10 minutes precipitation data for one station."""
    # common settings
    _MetaModel = MetaN
    _para = "n"
    _para_long = "Precipitation"
    _unit = "mm/10min"
    _valid_kinds = ["raw", "qn", "qc", "corr", "filled", "filled_by"]
    _best_kind = "corr"

    # cdc dwd parameters
    _ftp_folder_base = [
        "climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/"]
    _cdc_col_names_imp = ["RWS_10", "QN"]
    _db_col_names_imp = ["raw", "qn"]

    # timestamp configurations
    _tstp_format_db = "%Y%m%d %H:%M"
    _tstp_dtype = "timestamp"
    _interval = "10 min"

    # aggregation
    _min_agg_to = "10 min"

    def __init__(self, id, **kwargs):
        super().__init__(id, **kwargs)
        self.id_str = dwd_id_to_str(id)

    def _get_sql_new_qc(self, period):
        # create sql_format_dict
        sql_format_dict = dict(
            para=self._para, stid=self.id, para_long=self._para_long,
            decim=self._decimals,
            **period.get_sql_format_dict(
                format="'{}'".format(self._tstp_format_db)),
            limit=0.1*self._decimals) # don't delete values below 0.1mm/10min if they are consecutive

        # check if daily station is available
        sql_check_d = """
            SELECT EXISTS(
                SELECT *
                FROM information_schema.tables
                WHERE table_schema = 'timeseries'
                    AND table_name = '{stid}_{para}_d'
            );""".format(**sql_format_dict)
        with db_engine.connect() as con:
            daily_exists = con.execute(sqltxt(sql_check_d)).first()[0]

        # create sql for dates where the aggregated 10 minutes measurements are 0
        # althought the daily measurements are not 0
        # or where the aggregated daily sum is more than the double of the daily measurement, when the daily measurement is more than 10 mm
        if daily_exists:
            sql_dates_failed = """
                WITH ts_10min_d AS (
                    SELECT (ts.timestamp - INTERVAL '6h')::date as date, sum("raw") as raw
                    FROM timeseries."{stid}_{para}" ts
                    WHERE ts.timestamp BETWEEN {min_tstp} AND {max_tstp}
                    GROUP BY (ts.timestamp - INTERVAL '6h')::date)
                SELECT date
                FROM timeseries."{stid}_{para}_d" ts_d
                LEFT JOIN ts_10min_d ON ts_d.timestamp::date=ts_10min_d.date
                WHERE ts_d.timestamp BETWEEN {min_tstp}::date AND {max_tstp}::date
                    AND ((ts_10min_d.raw = 0 AND ts_d.raw <> 0) OR
                         (ts_10min_d.raw >= 10*{decim} AND ts_10min_d.raw >= (ts_d.raw*2)))
            """.format(**sql_format_dict)
        else:
            log.warn((
                "For the {para_long} station with ID {stid} there is no timeserie with daily values. " +
                "Therefor the quality check for daily values equal to 0 is not done.\n" +
                "Please consider updating the daily stations with:\n" +
                "stats = stations.StationsND()\n" +
                "stats.update_meta()\nstats.update_raw()"
            ).format(**sql_format_dict))
            sql_dates_failed = """
                SELECT NULL::date as date
            """

        # remove single peaks above 5mm/10min
        sql_single_peaks = """
            SELECT ts.timestamp
            FROM timeseries."{stid}_{para}" ts
            INNER JOIN timeseries."{stid}_{para}" tsb
                ON ts.timestamp = tsb.timestamp - INTERVAL '10 min'
            INNER JOIN timeseries."{stid}_{para}" tsa
                ON ts.timestamp = tsa.timestamp + INTERVAL '10 min'
            WHERE ts.raw > (5*{decim}) AND tsb.raw = 0 AND tsa.raw = 0
                AND ts.timestamp BETWEEN {min_tstp} AND {max_tstp}
        """.format(**sql_format_dict)

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
            UNION ({sql_single_peaks})
        """.format(sql_single_peaks=sql_single_peaks,
                   **sql_format_dict)

        # create sql for new qc values
        sql_new_qc = """
            WITH tstps_failed as ({sql_tstps_failed}),
                 dates_failed AS ({sql_dates_failed})
            SELECT ts.timestamp,
                (CASE WHEN ((ts.timestamp IN (SELECT timestamp FROM tstps_failed))
                        OR ((ts.timestamp - INTERVAL '6h')::date IN (
                            SELECT date FROM dates_failed))
                        OR ts."raw" < 0
                        OR ts."raw" >= 50*{decim})
                    THEN NULL
                    ELSE ts."raw" END) as qc
            FROM timeseries."{stid}_{para}" ts
            WHERE ts.timestamp BETWEEN {min_tstp} AND {max_tstp}
        """.format(
            sql_tstps_failed=sql_tstps_failed,
            sql_dates_failed=sql_dates_failed,
            **sql_format_dict)

        return sql_new_qc

    def _check_df_raw(self, df):
        """This function is used in the Base class on the single dataframe that is downloaded from the CDC Server before loading it in the database.

        Here the function adapts the timezone relative to the date.
        As the data on the CDC server is in MEZ before 200 and in UTC after 2000

        Some precipitation stations on the DWD CDC server have also rows outside of the normal 10 Minute frequency, e.g. 2008-09-16 01:47 for Station 662.
        Because those rows only have NAs for the measurement they are deleted."""
        # correct Timezone before 2000 -> MEZ after 2000 -> UTC
        if df.index.min() >= pd.Timestamp(1999,12,31,23,0):
            df.index = df.index.tz_localize("UTC")
        elif df.index.max() < pd.Timestamp(2000,1,1,0,0):
            df.index = df.index.tz_localize("Etc/GMT+1").tz_convert("UTC")
        else:
            raise ValueError("The timezone could not get defined for the given import." + str(df))

        # delete measurements outside of the 10 minutes frequency
        df = df[df.index.minute%10==0].copy()
        df = df.asfreq("10min")

        # delete measurements below 0
        n_col = self._cdc_col_names_imp[self._db_col_names_imp.index("raw")]
        df.loc[df[n_col]<0, n_col] = np.nan

        return df

    @db_engine.deco_create_privilege
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
        with db_engine.connect() as con:
            con.execute(sqltxt(sql_add_table))
            con.commit()

    @staticmethod
    def _check_period_extra(period):
        """Additional checks on period used in StationBase class _check_period method."""
        # add time to period if given as date
        return period.expand_to_timestamp()

    @staticmethod
    def _richter_class_from_horizon(horizon):
        richter_class = None
        for key in RICHTER_CLASSES:
            if (horizon >= RICHTER_CLASSES[key]["min_horizon"]) and \
                (horizon < RICHTER_CLASSES[key]["max_horizon"]):
                richter_class = key
        return richter_class

    @db_engine.deco_update_privilege
    def update_horizon(self, skip_if_exist=True, **kwargs):
        """Update the horizon angle (Horizontabschirmung) in the meta table.

        Get new values from the raster and put in the table.

        Parameters
        ----------
        skip_if_exist : bool, optional
            Skip updating the value if there is already a value in the meta table.
            The default is True.
        kwargs : dict, optional
            Additional keyword arguments catch all, but unused here.

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
        dem_files = [Path(file) for file in config.getlist("data:rasters", "dems")]
        for dem_file in dem_files:
            if dem_file.is_file():
                raise ValueError(
                    f"The DEM file was not found in the data directory under: \n{dem_file}")

        # get the horizon value
        radius = 75000 # this value got defined because the maximum height is around 4000m for germany

        with rio.open(dem_files[0]) as dgm1,\
            rio.open(dem_files[1]) if len(dem_files)>1 else nullcontext() as dgm2:
            # sample station heights from the first DGM
            geom1 = self.get_geom(crs=dgm1.crs.to_epsg())
            xy = [geom1.x, geom1.y]
            stat_h1 = list(dgm1.sample(
                xy=[xy],
                indexes=1,
                masked=True))[0]
            if stat_h1.mask[0]:
                log.error(
                    f"update_horizon(): No height was found in the first DGM for {self._para_long} Station {self.id}. " +
                    "Therefor the horizon angle could not get updated.")
                raise ValueError()
            else:
                stat_h1 = stat_h1[0]

            # sample dgm for horizon angle
            hab = pd.Series(
                index=pd.Index([], name="angle", dtype=int),
                name="horizon",
                dtype=float)

            for angle in range(90, 271, 3):
                dgm1_mask = polar_line(xy, radius, angle)
                dgm1_np, dgm1_tr = rasterio.mask.mask(
                    dgm1, [dgm1_mask], crop=True)
                dgm1_np[dgm1_np==dgm1.profile["nodata"]] = np.nan
                dgm_gpd = raster2points(dgm1_np, dgm1_tr, crs=dgm1.crs)
                idx_min = dgm_gpd.distance(geom1).idxmin()
                geom_dgm1 = dgm_gpd.loc[idx_min, "geometry"]
                dgm_gpd.drop(idx_min, inplace=True)
                dgm_gpd["dist"] = dgm_gpd.distance(geom_dgm1)
                dgm_gpd = dgm_gpd.sort_values("dist").reset_index(drop=True)
                dgm_gpd["horizon"] = np.degrees(np.arctan(
                        (dgm_gpd["data"]-stat_h1) / dgm_gpd["dist"]))

                # check if parts are missing and fill
                #####################################
                line_parts = pd.DataFrame(
                    columns=["Start_point", "radius", "line"])

                # look for holes inside the line
                for i, j in enumerate(dgm_gpd[dgm_gpd["dist"].diff() > dgm1_tr[0]*np.sqrt(2)].index):
                    line_parts = pd.concat(
                        [line_parts,
                        pd.DataFrame(
                            {"Start_point": dgm_gpd.loc[j-1, "geometry"],
                            "radius": dgm_gpd.loc[j, "dist"] - dgm_gpd.loc[j-1, "dist"]},
                            index=[i])])

                # look for missing values at the end
                dgm1_max_dist = dgm_gpd.iloc[-1]["dist"]
                if dgm1_max_dist < (radius - dgm1_tr[0]/2*np.sqrt(2)):
                    line_parts = pd.concat(
                        [line_parts,
                        pd.DataFrame(
                            {"Start_point":  dgm_gpd.iloc[-1]["geometry"],
                             "radius": radius - dgm1_max_dist},
                            index=[line_parts.index.max()+1])])

                # check if parts are missing and fill
                if len(line_parts) > 0 & (dgm2 is not None):
                    # sample station heights from the second DGM
                    geom2 = self.get_geom(crs=dgm2.crs.to_epsg())
                    stat_h2 = list(dgm2.sample(
                        xy=[(geom2.x, geom2.y)],
                        indexes=1,
                        masked=True))[0]
                    if stat_h2.mask[0]:
                        log.error(
                            f"update_horizon(): No height was found in the second DGM for {self._para_long} Station {self.id}. " +
                            "Therefor the height from the first DGM is taken also for the second.")
                        stat_h2 = stat_h1
                    else:
                        stat_h2 = stat_h2[0]

                    # create the lines
                    for i, row in line_parts.iterrows():
                        line_parts.loc[i, "line"] = polar_line(
                            [el[0] for el in row["Start_point"].xy],
                            row["radius"],
                            angle
                        )
                    line_parts = gpd.GeoDataFrame(
                        line_parts, geometry="line", crs=dgm1.crs
                        ).to_crs(dgm2.crs)
                    dgm2_mask = MultiLineString(
                        line_parts["line"].tolist())
                    dgm2_np, dgm2_tr = rasterio.mask.mask(
                        dgm2, [geom2, dgm2_mask], crop=True)
                    dgm2_np[dgm2_np==dgm2.profile["nodata"]] = np.nan
                    dgm2_gpd = raster2points(
                        dgm2_np, dgm2_tr, crs=dgm2.crs
                        )
                    idx_min = dgm2_gpd.distance(geom2).idxmin()
                    geom_dgm2 = dgm2_gpd.loc[idx_min, "geometry"]
                    dgm2_gpd.drop(idx_min, inplace=True)

                    dgm2_gpd["dist"] = dgm2_gpd.distance(geom_dgm2)
                    dgm2_gpd["horizon"] = np.degrees(np.arctan(
                        (dgm2_gpd["data"]-stat_h2) / dgm2_gpd["dist"]))
                    dgm_gpd = pd.concat(
                        [dgm_gpd[["horizon"]], dgm2_gpd[["horizon"]]], ignore_index=True)

                hab[angle] = dgm_gpd["horizon"].max()

        # calculate the mean "horizontabschimung"
        # Richter: H’=0,15H(S-SW) +0,35H(SW-W) +0,35H(W-NW) +0, 15H(NW-N)
        horizon = max(0,
            0.15*hab[(hab.index>225) & (hab.index<=270)].mean()
            + 0.35*hab[(hab.index>=180) & (hab.index<=225)].mean()
            + 0.35*hab[(hab.index>=135) & (hab.index<180)].mean()
            + 0.15*hab[(hab.index>=90) & (hab.index<135)].mean())

        # insert to meta table in db
        self._update_meta(
            cols=["horizon"],
            values=[horizon])

        return horizon

    @db_engine.deco_update_privilege
    def update_richter_class(self, skip_if_exist=True, **kwargs):
        """Update the richter class in the meta table.

        Get new values from the raster and put in the table.

        Parameters
        ----------
        skip_if_exist : bool, optional
            Skip updating the value if there is already a value in the meta table.
            The default is True
        kwargs : dict, optional
            Additional keyword arguments catch all, but unused here.

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

    @db_engine.deco_update_privilege
    def richter_correct(self, period=(None, None), **kwargs):
        """Do the richter correction on the filled data for the given period.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        kwargs : dict, optional
            Additional keyword arguments catch all, but unused here.

        Raises
        ------
        Exception
            If no richter class was found for this station.
        """
        # check if period is given
        if not isinstance(period, TimestampPeriod):
            period = TimestampPeriod(*period)
        period_in = period.copy()
        period = self._check_period(
                period=period, kinds=["filled"])
        if not period_in.is_empty():
            sql_period_clause = \
                "WHERE timestamp BETWEEN {min_tstp} AND {max_tstp}".format(
                    **period.get_sql_format_dict(f"'{self._tstp_format_db}'"))
        else:
            sql_period_clause = ""

        # check if temperature station is filled
        stat_t = StationT(self.id)
        stat_t_period = stat_t.get_filled_period(kind="filled")
        delta = timedelta(hours=5, minutes=50)
        min_date = pd.Timestamp(MIN_TSTP).date()
        stat_t_min = stat_t_period[0].date()
        stat_t_max = stat_t_period[1].date()
        stat_n_min = (period[0] - delta).date()
        stat_n_max = (period[1] - delta).date()
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
            raise Exception("No richter class was found for the precipitation station {stid} and therefor no richter correction was possible."\
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
            SELECT timestamp::date AS date,
                   sum("filled") AS "filled",
                   count(*) FILTER (WHERE "filled" > 0) AS "count_n"
            FROM timeseries."{stid}_{para}"
            {period_clause}
            GROUP BY timestamp::date
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
            FROM ({sql_n_daily_winter}) tsn_d_wi
            LEFT JOIN timeseries."{stid}_t" tst
                ON tst.timestamp=tsn_d_wi.date
        """.format(
            sql_n_daily_winter=sql_n_daily_winter,
            **sql_format_dict
        )

        # calculate the delta n
        sql_delta_n = """
            SELECT date,
                CASE WHEN "count_n"> 0 THEN
                        round(("b_{richter_class}" * ("n_d"::float/{n_decim})^"E" * {n_decim})/"count_n")::int
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
        sql_new_corr = """
            SELECT timestamp,
                CASE WHEN "filled" > 0
                     THEN ts."filled" + ts_delta_n."delta_10min"
                     ELSE ts."filled"
                END as corr
            FROM timeseries."{stid}_{para}" ts
            LEFT JOIN ({sql_delta_n}) ts_delta_n
                ON (ts.timestamp)::date = ts_delta_n.date
            {period_clause}
        """.format(
            sql_delta_n=sql_delta_n,
            **sql_format_dict
        )

        # update the timeseries
        sql_update = """
            UPDATE timeseries."{stid}_{para}" ts
            SET "corr" = new.corr
            FROM ({sql_new_corr}) new
            WHERE ts.timestamp = new.timestamp
                AND ts.corr is distinct from new.corr;
        """.format(
            sql_new_corr=sql_new_corr,
            **sql_format_dict
        )

        # run commands
        if "return_sql" in kwargs and kwargs["return_sql"]:
            return sql_update

        self._execute_long_sql(
            sql_update,
            description="richter corrected for the period {min_tstp} - {max_tstp}".format(
                **period.get_sql_format_dict(format=self._tstp_format_human)
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

            with db_engine.connect() as con:
                con.execute(sqltxt(sql_diff_filled))

        # update filled time in meta table
        self.update_period_meta(kind="corr")

    @db_engine.deco_update_privilege
    def corr(self, *args, **kwargs):
        return self.richter_correct(*args, **kwargs)

    @db_engine.deco_update_privilege
    def last_imp_richter_correct(self, _last_imp_period=None, **kwargs):
        """Do the richter correction of the last import.

        Parameters
        ----------
        _last_imp_period : weatherDB.utils.TimestampPeriod, optional
            Give the overall period of the last import.
            This is only for intern use of the stationsN method to not compute over and over again the period.
            The default is None.
        kwargs : dict, optional
            Additional keyword arguments passed to the richter_correct method.
        """
        if not self.is_last_imp_done(kind="corr"):
            if _last_imp_period is None:
                period = self.get_last_imp_period(all=True)
            else:
                period = _last_imp_period

            self.richter_correct(
                period=period,
                **kwargs
            )

        else:
            log.info("The last import of {para_long} Station {stid} was already richter corrected and is therefor skiped".format(
                stid=self.id, para_long=self._para_long
            ))

    @db_engine.deco_update_privilege
    def last_imp_corr(self, _last_imp_period=None, **kwargs):
        """A wrapper for last_imp_richter_correct()."""
        return self.last_imp_richter_correct(_last_imp_period=_last_imp_period, **kwargs)

    @db_engine.deco_update_privilege
    def _sql_fillup_extra_dict(self, **kwargs):
        fillup_extra_dict = super()._sql_fillup_extra_dict(**kwargs)

        stat_nd = StationND(self.id)
        if stat_nd.isin_db() and \
                not stat_nd.get_filled_period(kind="filled", from_meta=True).is_empty():
            # adjust 10 minutes sum to match measured daily value,
            # but don't add more than 10mm/10min and don't create single peaks with more than 5mm/min
            sql_extra = """
                UPDATE new_filled_{stid}_{para} ts
                SET filled = tsnew.filled
                FROM (
                    SELECT ts.timestamp,
                        CASE WHEN tsb.filled = 0 AND tsa.filled = 0
                             THEN LEAST(ts.filled * coef, 5*{decim})
                             ELSE CASE WHEN ((ts.filled * coef) - ts.filled) <= (10 * {decim})
                                       THEN LEAST(ts.filled * coef, 50*{decim})
                                       ELSE LEAST(ts.filled + (10 * {decim}), 50*{decim})
                                  END
                             END as filled
                    FROM new_filled_{stid}_{para} ts
                    INNER JOIN (
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
                        ON (ts.timestamp - '5h 50min'::INTERVAL)::date = df_coef.date
                           AND coef != 1
                    LEFT JOIN timeseries."{stid}_{para}" tsb
                        ON ts.timestamp = tsb.timestamp - INTERVAL '10 min'
                    LEFT JOIN timeseries."{stid}_{para}" tsa
                        ON ts.timestamp = tsa.timestamp + INTERVAL '10 min'
                ) tsnew
                WHERE tsnew.timestamp = ts.timestamp;
            """.format(stid=self.id, para=self._para, decim=self._decimals)

            fillup_extra_dict.update(dict(sql_extra_after_loop=sql_extra))
        else:
            log.warn("Station_N({stid}).fillup: There is no daily timeserie in the database, "+
                     "therefor the 10 minutes values are not getting adjusted to daily values")

        return fillup_extra_dict

    @db_engine.deco_update_privilege
    def fillup(self, period=(None, None), **kwargs):
        super_ret = super().fillup(period=period, **kwargs)

        # check the period
        if not isinstance(period, TimestampPeriod):
            period= TimestampPeriod(*period)

        # update difference to dwd_grid and hyras
        if period.is_empty() or period[0].year < pd.Timestamp.now().year:
            sql_diff_ma = """
                UPDATE meta_n
                SET quot_filled_dwd_grid = quots.quot_dwd*100,
                    quot_filled_hyras = quots.quot_hyras*100
                FROM (
                    SELECT df_ma.ys / (srv.n_dwd_year*{decimals}) AS quot_dwd,
                        df_ma.ys / (srv.n_hyras_year*{decimals}) AS quot_hyras
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
            """.format(
                stid=self.id,
                para=self._para,
                decimals=self._decimals)

            #execute sql or return
            if "return_sql" in kwargs and kwargs["return_sql"]:
                return (str(super_ret) + "\n" + sql_diff_ma)
            with db_engine.connect() as con:
                con.execute(sqltxt(sql_diff_ma))

    def get_corr(self, **kwargs):
        return self.get_df(kinds=["corr"], **kwargs)

    def get_qn(self, **kwargs):
        return self.get_df(kinds=["qn"], **kwargs)

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

        with db_engine.connect() as con:
            res = con.execute(sqltxt(sql)).first()

        # check result
        if res is None:
            if update_if_fails:
                if db_engine.is_superuser:
                    self.update_richter_class()
                    # update_if_fails is False to not get an endless loop
                    return self.get_richter_class(update_if_fails=False)
                else:
                    warnings.warn("You don't have the permissions to change something on the database.\nTherefor an update of the richter_class is not possible.")
                    return None
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
        return self.get_meta(infos="horizon")

