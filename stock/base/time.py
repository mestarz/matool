from enum import Enum


class TimeLevel(Enum):
    Min5 = 1
    Min15 = 2
    Hour1 = 3
    Hour4 = 4
    Day1 = 5
    Week1 = 6
    Month1 = 7

    def get_baostock_freq(self):
        freq_tab = {
            TimeLevel.Min5: "5",
            TimeLevel.Min15: "15",
            TimeLevel.Hour1: "60",
            TimeLevel.Day1: "d",
            TimeLevel.Week1: "w",
            TimeLevel.Month1: "m",
        }
        return freq_tab[self]

    def is_day_or_more(self) -> bool:
        return self in [
            TimeLevel.Day1,
            TimeLevel.Week1,
            TimeLevel.Month1
        ]
