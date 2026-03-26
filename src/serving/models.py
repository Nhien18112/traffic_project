from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class TrafficRecord(BaseModel):
    event_time:        datetime
    location_name:     str
    latitude:          Optional[float] = None
    longitude:         Optional[float] = None
    weather_condition: Optional[str]   = None
    temperature:       Optional[float] = None
    current_speed:     Optional[float] = None
    free_flow_speed:   Optional[float] = None
    speed_ratio:       Optional[float] = None
    congestion_level:  Optional[int]   = None
    congestion_label:  Optional[str]   = None
    motorcycle_count:  Optional[int]   = None
    car_count:         Optional[int]   = None
    bus_truck_count:   Optional[int]   = None

    class Config:
        from_attributes = True


class LocationSummary(BaseModel):
    location_name:      str
    avg_speed:          Optional[float] = None
    avg_speed_ratio:    Optional[float] = None
    most_common_status: Optional[str]   = None
    total_motorcycle:   Optional[int]   = None
    total_car:          Optional[int]   = None
    total_bus_truck:    Optional[int]   = None
    total_vehicles:     Optional[int]   = None
    total_records:      int

    class Config:
        from_attributes = True


class WeatherImpact(BaseModel):
    weather_condition: str
    avg_speed:         Optional[float] = None
    avg_temperature:   Optional[float] = None
    avg_speed_ratio:   Optional[float] = None
    sample_count:      int

    class Config:
        from_attributes = True


class ChartPoint(BaseModel):
    bucket:    datetime
    avg_speed: Optional[float] = None
    avg_ratio: Optional[float] = None

    class Config:
        from_attributes = True