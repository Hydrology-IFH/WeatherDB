import sqlalchemy as sa
from sqlalchemy.sql import func
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from datetime import datetime, timedelta
from typing import Optional
from typing_extensions import Annotated
from geoalchemy2 import Geometry

# define custom types
sint = Annotated[int, 2]
str30 = Annotated[str, 30]

# define base class for all database tables
class Base(DeclarativeBase):
    registry = sa.orm.registry(
        type_annotation_map={
            sint: sa.SmallInteger(),
            datetime: sa.TIMESTAMP(),
            float: sa.FLOAT(),
            str30: sa.CHAR(30),
        }
    )

class MetaBase(Base):
    __abstract__ = True

    station_id: Mapped[int] = mapped_column(
        primary_key=True,
        comment="official DWD-ID of the station")
    is_real: Mapped[bool] = mapped_column(
        default=True,
        comment=" 'Is this station a real station with own measurements or only a virtual station, to have complete timeseries for every precipitation station.")
    raw_from: Mapped[Optional[datetime]] = mapped_column(
        comment="The timestamp from when on own \"raw\" data is available")
    raw_until: Mapped[Optional[datetime]] = mapped_column(
        comment="The timestamp until when own \"raw\" data is available")
    hist_until: Mapped[Optional[datetime]] = mapped_column(
        comment="The timestamp until when own \"raw\" data is available")
    filled_from: Mapped[Optional[datetime]] = mapped_column(
        comment="The timestamp from when on filled data is available")
    filled_until: Mapped[Optional[datetime]] = mapped_column(
        comment="The timestamp until when filled data is available")
    last_imp_from: Mapped[Optional[datetime]] = mapped_column(
        comment="The minimal timestamp of the last import, that might not yet have been treated")
    last_imp_until: Mapped[Optional[datetime]] = mapped_column(
        comment="The maximal timestamp of the last import, that might not yet have been treated")
    last_imp_filled: Mapped[bool] = mapped_column(
        default=False,
        comment="Got the last import already filled?")
    stationshoehe: Mapped[int] = mapped_column(
        comment="The stations height above the ground in meters")
    stationsname: Mapped[str30] = mapped_column(
        comment="The stations official name as text")
    bundesland: Mapped[str30] = mapped_column(
        comment="The state the station is located in")
    geometry: Mapped[str] = mapped_column(
        Geometry('POINT', 4326),
        comment="The stations location in the WGS84 coordinate reference system (EPSG:4326)")
    geometry_utm: Mapped[str] = mapped_column(
        Geometry('POINT', 25832),
        comment="The stations location in the UTM32 coordinate reference system (EPSG:25832)")

class MetaBaseQC(Base):
    __abstract__ = True

    last_imp_qc: Mapped[bool] = mapped_column(
        default=False,
        comment="Got the last import already quality checked?")
    qc_from: Mapped[Optional[datetime]] = mapped_column(
        comment="The timestamp from when on quality checked(\"qc\") data is available")
    qc_until: Mapped[Optional[datetime]] = mapped_column(
        comment="The timestamp until when quality checked(\"qc\") data is available")
    qc_droped: Mapped[float] = mapped_column(
        comment="The percentage of droped values during the quality check")

# declare all database tables
#----------------------------
class MetaN(MetaBase, MetaBaseQC):
    __tablename__ = 'meta_n'
    __table_args__ = dict(
        comment="The Meta informations of the precipitation stations.")

    last_imp_corr: Mapped[bool] = mapped_column(
        default=False,
        comment="Got the last import already Richter corrected?")
    corr_from: Mapped[Optional[datetime]] = mapped_column(
        comment="The timestamp from when on corrected data is available")
    corr_until: Mapped[Optional[datetime]] = mapped_column(
        comment="The timestamp until when corrected data is available")
    horizon: Mapped[float] = mapped_column(
        comment="The horizon angle in degrees, how it got defined by Richter(1995).")
    richter_class: Mapped[str] = mapped_column(
        sa.String(),
        comment="The Richter exposition class, that got derived from the horizon angle.")
    quot_filled_hyras: Mapped[float] = mapped_column(
        comment="The quotient betwen the mean yearly value from the filled timeserie to the multi annual yearly mean HYRAS value (1991-2020)")
    quot_filled_regnie: Mapped[float] = mapped_column(
        comment="The quotient betwen the mean yearly value from the filled timeserie to the multi annual yearly mean REGNIE value (1991-2020)")
    quot_filled_dwd_grid: Mapped[float] = mapped_column(
        comment="The quotient betwen the mean yearly value from the filled timeserie to the multi annual yearly mean DWD grid value (1991-2020)")
    quot_corr_filled: Mapped[float] = mapped_column(
        comment="The quotient betwen the mean yearly value from the Richter corrected timeserie to the mean yearly value from the filled timeserie")

class MetaND(MetaBase):
    __tablename__ = 'meta_n_d'


class MetaET(MetaBase, MetaBaseQC):
    __tablename__ = 'meta_et'


class MetaT(MetaBase, MetaBaseQC):
    __tablename__ = 'meta_t'


class RawFiles(Base):
    __tablename__ = 'raw_files'
    __table_args__ = dict(
        comment="The files that got imported from the CDC Server.")

    para: Mapped[str] = mapped_column(
        primary_key=True,
        comment="The parameter that got downloaded for this file. e.g. t, et, n_d, n")
    filepath: Mapped[str] = mapped_column(
        primary_key=True,
        comment="The filepath on the CDC Server")
    modtime: Mapped[datetime] = mapped_column(
        comment="The modification time on the CDC Server of the coresponding file")


class DropedStations(Base):
    __tablename__ = 'droped_stations'
    __table_args__ = dict(
        comment="This table is there to save the station ids that got droped, so they wont GET recreated")

    station_id: Mapped[int] = mapped_column(
        primary_key=True,
        comment="The station id that got droped")
    para: Mapped[str] = mapped_column(
        sa.CHAR(3),
        primary_key=True,
        comment="The parameter (n,t,et,n_d) of the station that got droped")
    why: Mapped[str] = mapped_column(
        sa.Text(),
        comment="The reason why the station got droped")
    timestamp: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        comment="The timestamp when the station got droped")


class ParaVariables(Base):
    __tablename__ = 'para_variables'
    __table_args__ = dict(
        comment="This table is there to save specific variables that are nescesary for the functioning of the scripts")

    para: Mapped[str] = mapped_column(
        sa.String(3),
        primary_key=True,
        comment="The parameter for which the variables are valid. e.g. n/n_d/t/et.")
    start_tstp_last_imp: Mapped[Optional[datetime]] = mapped_column(
        comment="At what timestamp did the last complete import start. This is then the maximum timestamp for which to expand the timeseries to.")
    max_tstp_last_imp: Mapped[Optional[datetime]] = mapped_column(
        comment="The maximal timestamp of the last imports raw data of all the timeseries")


class RichterValues(Base):
    __tablename__ = 'richter_values'
    __table_args__ = dict(
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


class StationsRasterValues(Base):
    __tablename__ = 'stations_raster_values'

    station_id: Mapped[int] = mapped_column(
        primary_key=True,
        comment="The DWD-ID of the station.")
    n_dwd_wihj: Mapped[sint] = mapped_column(
        comment="The multi-annual mean precipitation sum of the winter half-year from the multi-annual DWD grid (1991-2020).")
    n_dwd_sohj: Mapped[sint] = mapped_column(
        comment="The multi-annual mean precipitation sum of the summer half-year from the multi-annual DWD grid (1991-2020).")
    n_dwd_year: Mapped[sint] = mapped_column(
        comment="The multi-annual mean yearly precipitation sum from the multi-annual DWD grid (1991-2020).")
    t_dwd_year: Mapped[int] = mapped_column(
        comment="The multi-annual mean yearly temperature from the multi-annual DWD grid (1991-2020).")
    et_dwd_year: Mapped[sint] = mapped_column(
        comment="The multi-annual mean yearly evapotranspiration from the multi-annual DWD grid (1991-2020).")
    dist_dwd: Mapped[sint] = mapped_column(
        comment="The distance to the nearest DWD grid cell.")
    n_regnie_wihj: Mapped[sint] = mapped_column(
        comment="The multi-annual mean precipitation sum of the winter half-year from the multi-annual REGNIE grid (1991-2020).")
    n_regnie_sohj: Mapped[sint] = mapped_column(
        comment="The multi-annual mean precipitation sum of the summer half-year from the multi-annual REGNIE grid (1991-2020).")
    n_regnie_year: Mapped[sint] = mapped_column(
        comment="The multi-annual mean yearly precipitation sum from the multi-annual REGNIE grid (1991-2020).")
    dist_regnie: Mapped[sint] = mapped_column(
        comment="The distance to the nearest REGNIE grid cell.")
    n_hyras_wihj: Mapped[sint] = mapped_column(
        comment="The multi-annual mean precipitation sum of the winter half-year from the multi-annual HYRAS grid (1991-2020).")
    n_hyras_sohj: Mapped[sint] = mapped_column(
        comment="The multi-annual mean precipitation sum of the summer half-year from the multi-annual HYRAS grid (1991-2020).")
    n_hyras_year: Mapped[sint] = mapped_column(
        comment="The multi-annual mean yearly precipitation sum from the multi-annual HYRAS grid (1991-2020).")
    dist_hyras: Mapped[sint] = mapped_column(
        comment="The distance to the nearest HYRAS grid cell.")
    s_r_f: Mapped[float] = mapped_column(
        comment="The solar radiation factor (SRF) of the station. Calculated as the quotient between the solar radiation of the DEM grid to a flat DEM.")
    r_s: Mapped[float] = mapped_column(
        comment="The global solar radiation at the stations location used to calculate the SRF.")
    dist_sol: Mapped[sint] = mapped_column(
        comment="The distance to the nearest solar radiation grid cell.")
    geometry: Mapped[str] = mapped_column(
        Geometry('POINT', 25832),
        comment="The stations location in the UTM32 coordinate reference system (EPSG:25832)")


class NeededDownloadTime(Base):
    __tablename__ = 'needed_download_time'
    __table_args__ = dict(
        comment="Saves the time needed to save the timeseries. This helps predicting download time")
    timestamp: Mapped[datetime] = mapped_column(
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

