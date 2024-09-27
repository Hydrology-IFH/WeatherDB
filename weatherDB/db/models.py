import sqlalchemy as sa
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm import DeclarativeBase
from typing_extensions import Annotated
from datetime import timedelta, timezone
from typing import Optional
from geoalchemy2 import Geometry

__all__ = [
    "MetaP",
    "MetaPD",
    "MetaET",
    "MetaT",
    "RawFiles",
    "DropedStations",
    "ParameterVariables",
    "RichterValues",
    "StationMATimeserie",
    "StationMARaster",
    "NeededDownloadTime",
    "Settings"
]


# define custom types
sint = Annotated[int, 2]
str3 = Annotated[str, 3]
str7 = Annotated[str, 7]
str30 = Annotated[str, 30]
str50 = Annotated[str, 50]

class UTCDateTime(sa.types.TypeDecorator):
    impl = sa.types.DateTime
    cache_ok = True

    def process_bind_param(self, value, engine):
        if value is None:
            return
        if value.utcoffset() is None:
            raise ValueError(
                'Got naive datetime while timezone-aware is expected'
            )
        return value.astimezone(timezone.utc)

    def process_result_value(self, value, engine):
        if value is not None:
            return value.replace(tzinfo=timezone.utc)

# define base class for all database tables
class ModelBase(DeclarativeBase):
    registry = sa.orm.registry(
        type_annotation_map={
            sint: sa.SmallInteger(),
            UTCDateTime: UTCDateTime(),
            float: sa.Float(),
            str3: sa.VARCHAR(3),
            str7: sa.VARCHAR(7),
            str30: sa.VARCHAR(30),
            str50: sa.VARCHAR(50),
        }
    )

# define database models
# ----------------------
class MetaBase(ModelBase):
    __abstract__ = True

    station_id: Mapped[int] = mapped_column(
        primary_key=True,
        comment="official DWD-ID of the station")
    is_real: Mapped[bool] = mapped_column(
        default=True,
        server_default=sa.sql.expression.true(),
        comment=" 'Is this station a real station with own measurements or only a virtual station, to have complete timeseries for every precipitation station.")
    raw_from: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The timestamp from when on own \"raw\" data is available")
    raw_until: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The timestamp until when own \"raw\" data is available")
    hist_until: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The timestamp until when own \"raw\" data is available")
    filled_from: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The timestamp from when on filled data is available")
    filled_until: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The timestamp until when filled data is available")
    last_imp_from: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The minimal timestamp of the last import, that might not yet have been treated")
    last_imp_until: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The maximal timestamp of the last import, that might not yet have been treated")
    last_imp_filled: Mapped[bool] = mapped_column(
        default=False,
        server_default=sa.sql.expression.false(),
        comment="Got the last import already filled?")
    stationshoehe: Mapped[int] = mapped_column(
        comment="The stations height above the ground in meters")
    stationsname: Mapped[str50] = mapped_column(
        comment="The stations official name as text")
    bundesland: Mapped[str30] = mapped_column(
        comment="The state the station is located in")
    geometry: Mapped[str] = mapped_column(
        Geometry('POINT', 4326),
        comment="The stations location in the WGS84 coordinate reference system (EPSG:4326)")
    geometry_utm: Mapped[str] = mapped_column(
        Geometry('POINT', 25832),
        comment="The stations location in the UTM32 coordinate reference system (EPSG:25832)")

class MetaBaseQC(ModelBase):
    __abstract__ = True

    last_imp_qc: Mapped[bool] = mapped_column(
        default=False,
        server_default=sa.sql.expression.false(),
        comment="Got the last import already quality checked?")
    qc_from: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The timestamp from when on quality checked(\"qc\") data is available")
    qc_until: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The timestamp until when quality checked(\"qc\") data is available")
    qc_droped: Mapped[Optional[float]] = mapped_column(
        comment="The percentage of droped values during the quality check")

# declare all database tables
# ---------------------------
class MetaP(MetaBase, MetaBaseQC):
    __tablename__ = 'meta_p'
    __table_args__ = dict(
        schema='public',
        comment="The Meta informations of the precipitation stations.")

    last_imp_corr: Mapped[bool] = mapped_column(
        default=False,
        server_default=sa.sql.expression.false(),
        comment="Got the last import already Richter corrected?")
    corr_from: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The timestamp from when on corrected data is available")
    corr_until: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The timestamp until when corrected data is available")
    horizon: Mapped[Optional[float]] = mapped_column(
        comment="The horizon angle in degrees, how it got defined by Richter(1995).")
    richter_class: Mapped[Optional[str]] = mapped_column(
        sa.String(),
        comment="The Richter exposition class, that got derived from the horizon angle.")


class MetaPD(MetaBase):
    __tablename__ = 'meta_p_d'
    __table_args__ = dict(
        schema='public',
        comment="The Meta informations of the daily precipitation stations.")


class MetaET(MetaBase, MetaBaseQC):
    __tablename__ = 'meta_et'
    __table_args__ = dict(
        schema='public',
        comment="The Meta informations of the evapotranspiration stations.")


class MetaT(MetaBase, MetaBaseQC):
    __tablename__ = 'meta_t'
    __table_args__ = dict(
        schema='public',
        comment="The Meta informations of the temperature stations.")


class RawFiles(ModelBase):
    __tablename__ = 'raw_files'
    __table_args__ = dict(
        schema='public',
        comment="The files that got imported from the CDC Server.")

    parameter: Mapped[str3] = mapped_column(
        primary_key=True,
        comment="The parameter that got downloaded for this file. e.g. t, et, p_d, n")
    filepath: Mapped[str] = mapped_column(
        primary_key=True,
        comment="The filepath on the CDC Server")
    modtime: Mapped[UTCDateTime] = mapped_column(
        comment="The modification time on the CDC Server of the coresponding file")


class DropedStations(ModelBase):
    __tablename__ = 'droped_stations'
    __table_args__ = dict(
        schema='public',
        comment="This table is there to save the station ids that got droped, so they wont GET recreated")

    station_id: Mapped[int] = mapped_column(
        primary_key=True,
        comment="The station id that got droped")
    parameter: Mapped[str3] = mapped_column(
        primary_key=True,
        comment="The parameter (n,t,et,p_d) of the station that got droped")
    why: Mapped[str] = mapped_column(
        sa.Text(),
        comment="The reason why the station got droped")
    timestamp: Mapped[UTCDateTime] = mapped_column(
        server_default=func.now(),
        comment="The timestamp when the station got droped")


class ParameterVariables(ModelBase):
    __tablename__ = 'parameter_variables'
    __table_args__ = dict(
        schema='public',
        comment="This table is there to save specific variables that are nescesary for the functioning of the scripts")

    parameter: Mapped[str3] = mapped_column(
        primary_key=True,
        comment="The parameter for which the variables are valid. e.g. n/p_d/t/et.")
    start_tstp_last_imp: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="At what timestamp did the last complete import start. This is then the maximum timestamp for which to expand the timeseries to.")
    max_tstp_last_imp: Mapped[Optional[UTCDateTime]] = mapped_column(
        comment="The maximal timestamp of the last imports raw data of all the timeseries")


class RichterValues(ModelBase):
    __tablename__ = 'richter_values'
    __table_args__ = dict(
        schema='public',
        comment="The Richter values for the equation.")

    precipitation_typ: Mapped[str] = mapped_column(
        sa.Text(),
        primary_key=True,
        comment="The type of precipitation. e.g. 'Schnee', 'Regen', ...")
    temperaturbereich: Mapped[str] = mapped_column(
        sa.Text(),
        comment="The temperature range.")
    e: Mapped[float] = mapped_column(
        comment="The e-value of the equation.")
    b_no_protection: Mapped[float] = mapped_column(
        name="b_no-protection",
        comment="The b-value of the equation for exposition class 'no protection'.")
    b_little_protection: Mapped[float] = mapped_column(
        name="b_little-protection",
        comment="The b-value of the equation for exposition class 'little protection'.")
    b_protected: Mapped[float] = mapped_column(
        comment="The b-value of the equation for exposition class 'protected'.")
    b_heavy_protection: Mapped[float] = mapped_column(
        name="b_heavy-protection",
        comment="The b-value of the equation for exposition class 'heavy protection'.")


class StationMATimeserie(ModelBase):
    __tablename__ = 'station_ma_timeserie'
    __table_args__ = dict(
        schema='public',
        comment="The multi annual mean values of the stations timeseries for the maximum available timespan.")
    station_id: Mapped[int] = mapped_column(
        primary_key=True,
        comment="The DWD-ID of the station.")
    parameter: Mapped[str3] = mapped_column(
        primary_key=True,
        comment="The parameter of the station. e.g. 'P', 'T', 'ET'")
    kind: Mapped[str] = mapped_column(
        primary_key=True,
        comment="The kind of the timeserie. e.g. 'raw', 'filled', 'corr'")
    value: Mapped[int] = mapped_column(
        comment="The multi annual value of the yearly mean value of the station to the multi annual mean value of the raster.")


class StationMARaster(ModelBase):
    __tablename__ = 'station_ma_raster'
    __table_args__ = dict(
        schema='public',
        comment="The multi annual climate raster values for each station.")

    station_id: Mapped[int] = mapped_column(
        primary_key=True,
        comment="The DWD-ID of the station.")
    raster_key: Mapped[str7] = mapped_column(
        primary_key=True,
        comment="The name of the raster. e.g. 'dwd' or 'hyras'")
    parameter: Mapped[str7] = mapped_column(
        primary_key=True,
        comment="The parameter of the raster. e.g. 'p_wihj', 'p_sohj', 'p_year', 't_year', 'et_year'")
    value: Mapped[int] = mapped_column(
        comment="The value of the raster for the station.")
    distance: Mapped[int] = mapped_column(
        comment="The distance of the station to the raster value in meters.")


class NeededDownloadTime(ModelBase):
    __tablename__ = 'needed_download_time'
    __table_args__ = dict(
        schema='public',
        comment="Saves the time needed to save the timeseries. This helps predicting download time")
    timestamp: Mapped[UTCDateTime] = mapped_column(
        server_default=func.now(),
        primary_key=True,
        comment="The timestamp when the download hapend.")
    quantity: Mapped[int] = mapped_column(
        comment="The number of stations that got downloaded")
    aggregate: Mapped[str] = mapped_column(
        comment="The chosen aggregation. e.g. hourly, 10min, daily, ...")
    timespan: Mapped[timedelta] = mapped_column(
        sa.Interval(),
        comment="The timespan of the downloaded timeseries. e.g. 2 years")
    zip: Mapped[bool] = mapped_column(
        comment="Was the download zipped?")
    pc: Mapped[str] = mapped_column(
        comment="The name of the pc that downloaded the timeseries.")
    duration: Mapped[timedelta] = mapped_column(
        sa.Interval(),
        comment="The needed time to download and create the timeserie")
    output_size: Mapped[int] = mapped_column(
        comment="The size of the created output file in bytes")


class Settings(ModelBase):
    __tablename__ = 'settings'
    __table_args__ = dict(
        schema='public',
        comment="This table saves settings values for the script-databse connection. E.G. the latest package version that updated the database.")
    key: Mapped[str] = mapped_column(
        sa.String(20),
        primary_key=True,
        comment="The key of the setting")
    value: Mapped[str] = mapped_column(
        sa.String(20),
        comment="The value of the setting")