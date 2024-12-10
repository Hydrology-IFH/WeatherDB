# libraries
import logging
from datetime import timedelta
from pathlib import Path
import warnings
import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text as sqltxt
from functools import cached_property
import rasterio as rio
import rasterio.mask
import pyproj
from shapely.ops import transform as shp_transform
from shapely import distance, Point

from ..db.connections import db_engine
from ..utils.dwd import  dwd_id_to_str
from ..utils.TimestampPeriod import TimestampPeriod
from ..utils.geometry import polar_line, raster2points
from ..config import config
from ..db.models import MetaP
from .StationBases import StationPBase
from .StationPD import StationPD
from .StationT import StationT

# set settings
# ############
__all__ = ["StationP"]
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
class StationP(StationPBase):
    """A class to work with and download 10 minutes precipitation data for one station."""
    # common settings
    _MetaModel = MetaP
    _para = "p"
    _para_base = _para
    _para_long = "Precipitation"
    _unit = "mm/10min"
    _valid_kinds = {"raw", "qn", "qc", "corr", "filled", "filled_by"}
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
            log.error((
                "For the {para_long} station with ID {stid} there is no timeserie with daily values. " +
                "Therefor the quality check for daily values equal to 0 is not done.\n" +
                "Please consider updating the daily stations with:\n" +
                "stats = stations.StationsPD()\n" +
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
        # correct Timezone before 2000 -> CEWT after 2000 -> UTC
        if df.index.tzinfo is not None:
            df.index = df.index.tz_localize(None) # some are already falsy localized
        if df.index.min()>= pd.Timestamp(1999,12,31,23,0):
            df.index = df.index.tz_localize("UTC")
        elif df.index.max() < pd.Timestamp(2000,1,1,1,0):
            df.index = df.index.tz_localize("Etc/GMT-1").tz_convert("UTC")
        else:
            raise ValueError("The timezone could not get defined for the given import." + str(df.head()))

        # delete measurements outside of the 10 minutes frequency
        df = df[df.index.minute%10==0].copy()

        # check if duplicates and try to remove
        if df.index.has_duplicates:
            df = df.reset_index()\
                .groupby(self._cdc_col_names_imp + [self._cdc_date_col])\
                .first()\
                .reset_index().set_index(self._cdc_date_col)
            if df.index.has_duplicates:
                raise ValueError("There are duplicates in the DWD data that couldn't get removed.")

        # set frequency to 10 minutes
        df = df.asfreq("10min")

        # delete measurements below 0
        n_col = self._cdc_col_names_imp[self._db_col_names_imp.index("raw")]
        df.loc[df[n_col]<0, n_col] = np.nan

        return df

    @cached_property
    def _table(self):
        return sa.table(
            f"{self.id}_{self._para}",
            sa.column("timestamp", sa.DateTime),
            sa.column("raw", sa.Integer),
            sa.column("qc", sa.Integer),
            sa.column("filled", sa.Integer),
            sa.column("filled_by", sa.SmallInteger),
            sa.column("corr", sa.Integer),
            schema="timeseries")

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
        **kwargs : dict, optional
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
        dem_files = [Path(file) for file in config.get_list("data:rasters", "dems")]
        for dem_file in dem_files:
            if not dem_file.is_file():
                raise ValueError(
                    f"The DEM file was not found in the data directory under: \n{dem_file}")

        # get the horizon configurations
        radius = int(config.get("weatherdb", "horizon_radius"))
        proj_crs = pyproj.CRS.from_epsg(config.get("weatherdb", "horizon_crs"))
        if not proj_crs.is_projected:
            raise ValueError(
                "The CRS for the horizon calculation is not projected. " +
                "Please define a projected CRS in the config file under weatherdb.horizon_crs.")

        # get raster basic information
        dem_infos = {}
        for dem_file in dem_files:
            with rio.open(dem_file) as dem:
                # get station position
                stat_h, stat_tr = rasterio.mask.mask(
                    dem, [self.get_geom(crs=dem.crs)],
                    crop=True, all_touched=False,
                    nodata=np.nan)
                stat_geom = raster2points(stat_h, stat_tr, crs=dem.crs)\
                    .to_crs(proj_crs)\
                    .iloc[0].geometry
                xy = [stat_geom.x, stat_geom.y]
                stat_h = stat_h.flatten()[0]

                # get CRS transformer
                tr_to_dem = pyproj.Transformer.from_crs(proj_crs, dem.crs, always_xy=True)
                tr_from_dem = pyproj.Transformer.from_crs(dem.crs, proj_crs, always_xy=True)

                # get typical distance of raster cell points
                dem_tr = dem.transform
                p_top_left = (dem_tr.c, dem_tr.f)
                p_top_right = (dem_tr.c+dem_tr.a*dem.profile["width"], dem_tr.f)
                p_bottom_left= (dem_tr.c, dem_tr.f+dem_tr.e*dem.profile["height"])
                p_bottom_right = (p_top_right[0], p_bottom_left[1])
                dists = []
                for p in [p_top_left, p_top_right, p_bottom_left, p_bottom_right]:
                    p1 = Point(tr_from_dem.transform(*p))
                    dists.append(distance(
                        p1, Point(tr_from_dem.transform(p[0] + dem_tr.a, p[1] + dem_tr.e))
                    ))

            dem_infos[dem_file] = dict(
                stat_geom=stat_geom,
                xy=xy,
                stat_h=stat_h,
                tr_to_dem=tr_to_dem,
                tr_from_dem=tr_from_dem,
                max_dist = max(dists)*1.2
                )

        # check if first dem has hight information
        if np.isnan(dem_infos[dem_files[0]]["stat_h"]):
            raise ValueError(
                f"update_horizon(): No height was found in the first dem for {self._para_long} Station {self.id}. " +
                "Therefor the horizon angle could not get updated.")

        # get station center point
        stat_geom = self.get_geom(crs=proj_crs)
        xy = [stat_geom.x, stat_geom.y]

        # compute the horizon angle for each western angle
        hab = pd.Series(
            index=pd.Index([], name="angle", dtype=int),
            name="horizon",
            dtype=float)
        raise_hole_error = False
        for angle in range(90, 271, 3):
            missing_lines = [polar_line(xy, radius, angle)]
            dem_lines = None
            for dem_file, dem_info in dem_infos.items():
                with rio.open(dem_file) as dem:
                    missing_lines_next = []
                    for line in missing_lines:
                        # get raster points
                        line_dem = shp_transform(
                            dem_info["tr_to_dem"].transform,
                            line)
                        dem_np, dem_tr = rasterio.mask.mask(
                            dem, [line_dem],
                            crop=True, all_touched=True,
                            nodata=np.nan)
                        dem_line = raster2points(dem_np, dem_tr, crs=dem.crs).to_crs(proj_crs)

                        # calculate the distance to the stations raster cell
                        dem_line["dist"] = dem_line.distance(dem_info["stat_geom"])
                        dem_line.drop(
                            dem_line.loc[dem_line["dist"]==0].index,
                            inplace=True)
                        dem_line = dem_line.sort_values("dist").reset_index(drop=True)


                        # calculate the horizon angle
                        dem_line["horizon"] = np.degrees(np.arctan(
                                (dem_line["data"]-dem_info["stat_h"]) / dem_line["dist"]))

                        # check if parts are missing and fill
                        #####################################

                        # look for holes inside the line
                        for i in dem_line[dem_line["dist"].diff() > dem_info["max_dist"]].index:
                            missing_lines_next.append(
                                polar_line(dem_line.loc[i-1, "geometry"].coords[0],
                                        dem_line.loc[i, "dist"] - dem_line.loc[i-1, "dist"],
                                        angle))

                        # merge the parts
                        dem_lines = pd.concat(
                            [dem_lines, dem_line], ignore_index=True)

                # look for missing values at the end
                dem_max_dist = dem_lines.iloc[-1]["dist"]
                if dem_max_dist < (radius - dem_info["max_dist"]):
                    missing_lines_next.append(
                        polar_line(dem_lines.iloc[-1]["geometry"].coords[0],
                                radius - dem_max_dist,
                                angle))

                # check if no parts are missing
                if (len(missing_lines_next) == 0):
                    break
                elif dem_file == dem_files[-1]:
                    raise_hole_error = True
                else:
                    # create lines for next iteration
                    missing_lines = missing_lines_next

            hab[angle] = dem_lines["horizon"].max()

        if raise_hole_error:
            log.warning(
                f"Station{self._para}({self.id}).update_horizon(): There were holes in the DEM rasters providen when calculating the horizon angle. Therefor the calculated horizon angle could be faulty, but doesn't have to be, if the station is close to the sea for example.")

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
        **kwargs : dict, optional
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
        **kwargs : dict, optional
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
        min_date = config.get_date("weatherdb", "min_date")
        stat_t_min = stat_t_period[0].date()
        stat_t_max = stat_t_period[1].date()
        stat_p_min = (period[0] - delta).date()
        stat_p_max = (period[1] - delta).date()
        if stat_t_period.is_empty()\
                or (stat_t_min > stat_p_min
                    and not (stat_p_min < min_date)
                             and (stat_t_min == min_date)) \
                or (stat_t_max  < stat_p_max)\
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
        sql_p_daily = """
            SELECT timestamp::date AS date,
                   sum("filled") AS "filled",
                   count(*) FILTER (WHERE "filled" > 0) AS "count_n"
            FROM timeseries."{stid}_{para}"
            {period_clause}
            GROUP BY timestamp::date
        """.format(**sql_format_dict)

        # add is_winter
        sql_p_daily_winter = """
            SELECT date,
			    CASE WHEN EXTRACT(MONTH FROM date) IN (1, 2, 3, 10, 11, 12)
			            THEN true::bool
			            ELSE false::bool
			    END AS is_winter,
			    "filled" AS "p_d",
			    "count_n"
			FROM ({sql_p_daily}) tsp_d
        """.format(sql_p_daily=sql_p_daily)

        # add precipitation class
        sql_p_daily_precip_class = """
            SELECT
                date, "count_n", "p_d",
                CASE WHEN (tst."filled" >= (3 * {t_decim}) AND "is_winter") THEN 'precip_winter'
                    WHEN (tst."filled" >= (3 * {t_decim}) AND NOT "is_winter") THEN 'precip_summer'
                    WHEN (tst."filled" <= (-0.7 * {t_decim})::int) THEN 'snow'
                    WHEN (tst."filled" IS NULL) THEN NULL
                    ELSE 'mix'
                END AS precipitation_typ
            FROM ({sql_p_daily_winter}) tsp_d_wi
            LEFT JOIN timeseries."{stid}_t" tst
                ON tst.timestamp=tsp_d_wi.date
        """.format(
            sql_p_daily_winter=sql_p_daily_winter,
            **sql_format_dict
        )

        # calculate the delta n
        sql_delta_n = """
            SELECT date,
                CASE WHEN "count_n"> 0 THEN
                        round(("b_{richter_class}" * ("p_d"::float/{n_decim})^"e" * {n_decim})/"count_n")::int
                    ELSE 0
                END AS "delta_10min"
            FROM ({sql_p_daily_precip_class}) tsp_d2
            LEFT JOIN richter_parameters r
                ON r."precipitation_typ"=tsp_d2."precipitation_typ"
        """.format(
            sql_p_daily_precip_class=sql_p_daily_precip_class,
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

        # update filled time in meta table
        self.update_period_meta(kind="corr")

        # update multi annual mean
        self.update_ma_timeseries(kind="corr")

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
        **kwargs : dict, optional
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

        stat_pd = StationPD(self.id)
        if stat_pd.isin_db() and \
                not stat_pd.get_filled_period(kind="filled", from_meta=True).is_empty():
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
                        LEFT JOIN timeseries."{stid}_p_d" ts_d
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
                    WHERE ts.filled IS NOT NULL
                ) tsnew
                WHERE tsnew.timestamp = ts.timestamp;
            """.format(stid=self.id, para=self._para, decim=self._decimals)

            fillup_extra_dict.update(dict(sql_extra_after_loop=sql_extra))
        else:
            log.warning(f"StationP({self.id}).fillup: There is no daily timeserie in the database, "+
                        "therefor the 10 minutes values are not getting adjusted to daily values")

        return fillup_extra_dict

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

