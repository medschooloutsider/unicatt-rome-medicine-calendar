#!/usr/bin/env python3
"""Curated, source-backed dated events for the University of Tartu Year 1 station."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo


STATION_ID = "tartu-y1-lessons"
CALENDAR_NAME = "University of Tartu Medicine Year 1 Lessons"
TIMEZONE = "Europe/Tallinn"
SOURCE_URLS = [
    "https://ut.ee/en/curriculum/medicine",
    "https://ois2.ut.ee/#/curricula/118617",
    "https://meditsiiniteadused.ut.ee/sites/default/files/2024-05/Arst1_0.pdf",
    "https://meditsiiniteadused.ut.ee/sites/default/files/2025-02/arst1kursus_kevad2025.pdf",
    "https://wiki.ut.ee/spaces/programmijuhile/pages/147195113/Tunniplaani%2Botsing%2B-%2BTimetable%2Bsearch",
]
SOURCE_EVIDENCE = "Local official OIS course-version and timetable documents, cross-checked against the official Tartu faculty timetable PDFs; see the LM41 crosswalk report for paths and hashes."


def _event(
    uid_suffix: str,
    course_code: str,
    course_name: str,
    date_text: str,
    title: str,
    *,
    start_time: str | None = None,
    location: str = "",
) -> dict[str, Any]:
    day = date.fromisoformat(date_text)
    local_midnight = datetime.combine(day, time.min, tzinfo=ZoneInfo(TIMEZONE))
    offset = local_midnight.strftime("%z")[:3] + ":" + local_midnight.strftime("%z")[3:]
    if start_time:
        start = f"{date_text}T{start_time}:00{offset}"
        end = None
        timing = f"Start time {start_time}; end time is not stated in the source."
    else:
        start = f"{date_text}T00:00:00{offset}"
        end = f"{(day + timedelta(days=1)).isoformat()}T00:00:00{offset}"
        timing = "Date recovered; source does not state a clock time."
    description = f"University of Tartu Medicine Year 1, {course_name} ({course_code}). {timing} {SOURCE_EVIDENCE}"
    return {
        "uid": f"tartu-y1-{uid_suffix}@codex",
        "start": start,
        "end": end,
        "summary": f"[Tartu Y1] {course_name} - {title}",
        "location": location,
        "description": description,
        "status": None,
        "source_record_id": f"{course_code}:{date_text}:{uid_suffix}",
        "source_kind": "official_ois_exact_date",
        "source_date": date_text,
        "metadata": {
            "course_code": course_code,
            "course_name": course_name,
            "timezone": TIMEZONE,
            "all_day": start_time is None,
            "end_unknown": start_time is not None,
            "confidence": "verified_exact_date",
        },
    }


def source_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    biophysics = [
        ("2024-09-09", "Lecture 1: Introduction, measurement, functions"),
        ("2024-09-16", "Lecture 2: Cardio-respiratory biophysics"),
        ("2024-09-23", "Lecture 3: Derivatives, integrals, states and flow"),
        ("2024-09-30", "Lecture 4: Mechanics, oscillations and waves"),
        ("2024-10-07", "Lecture 5: Oscillations, waves and electric phenomena"),
        ("2024-10-14", "Lecture 6: Magnetic field, AC and bioelectromagnetism"),
        ("2024-10-21", "Lecture 7: Optics"),
        ("2024-10-28", "Lecture 8: Exponential functions and radioactivity"),
        ("2024-11-04", "Lecture 9: Transport in biological membranes"),
        ("2024-11-11", "Lecture 10: Resting and action potential"),
        ("2024-11-18", "Lecture 11: Skeleton and muscles"),
        ("2024-11-25", "Lecture 12: Information"),
        ("2024-12-02", "Lecture 13: Sensory systems, vision and hearing"),
        ("2024-12-09", "Lecture 14: Control, body functioning and homeostasis"),
    ]
    for index, (day, title) in enumerate(biophysics, 1):
        events.append(_event(f"biophysics-lecture-{index:02d}-{day}", "ARFS.01.023", "Biophysics", day, title, start_time="11:15", location="Biomedicum"))

    medchem = [
        ("2024-11-13", "Practical: Determination of the buffer capacity"),
        ("2024-11-20", "Practical: Determination of the rate constant"),
        ("2024-11-27", "Practical: Determination of the critical coagulation of the sol"),
        ("2024-12-04", "Practical: Study of adsorption of a dye on activated carbon"),
    ]
    for index, (day, title) in enumerate(medchem, 1):
        events.append(_event(f"medical-chemistry-practical-{index:02d}-{day}", "LOKT.00.009", "Medical Chemistry", day, title))

    biology_autumn = [
        ("2024-09-05", "Lecture 1: Molecular and cellular biology; cells and nucleic acids"),
        ("2024-09-12", "Lecture 2: DNA replication and transcription"),
        ("2024-09-19", "Lecture 3: Protein translation and protein structures"),
        ("2024-09-26", "Lecture 4: Genes and gene expression"),
        ("2024-10-03", "Lecture 5: Human genome and applications in medicine"),
        ("2024-10-16", "Lecture 6: Monogenic and polygenic inheritance"),
        ("2024-10-17", "Lecture 7: Human chromosomes"),
        ("2024-10-24", "Lecture 8: Plasma membrane and transport"),
        ("2024-10-31", "Lecture 9: Cell compartmentalization and intracellular transport"),
        ("2024-11-07", "Lecture 10: Mitochondria, energy and cytoskeleton"),
        ("2024-11-14", "Lecture 11: Cell junctions, adhesion and extracellular matrix"),
        ("2024-11-21", "Lecture 12: Proliferation, cell cycle and apoptosis"),
        ("2024-11-28", "Lecture 13: Signaling and transduction"),
        ("2024-12-05", "Lecture 14: Oncogenes, tumor suppressors and cancer"),
        ("2024-12-12", "Lecture 15: Molecular and cell biology methods; summary"),
    ]
    for index, (day, title) in enumerate(biology_autumn, 1):
        events.append(_event(f"biology-autumn-lecture-{index:02d}-{day}", "ARMP.01.029", "Biology Part 1", day, title))

    biology_spring = [
        ("2025-02-18", "Seminar: Protozoa"),
        ("2025-02-19", "Seminar: Toxoplasmosis and cryptosporidiosis"),
        ("2025-02-25", "Seminar: Plasmodium, malaria, Babesia and Balantidium"),
        ("2025-03-05", "Seminar: Helminths introduction and flukes"),
        ("2025-03-11", "Seminar: Tapeworms"),
        ("2025-03-12", "Seminar: Nematodes"),
        ("2025-03-18", "Seminar: Arthropods, vectors, ticks and mites"),
        ("2025-03-25", "Seminar: Insects, vectors and parasitic disease prevention"),
        ("2025-04-08", "Seminar: Training for microscopic slide examination"),
        ("2025-04-15", "Seminar: Microscopic slide examination, protozoa"),
    ]
    for index, (day, title) in enumerate(biology_spring, 1):
        events.append(_event(f"biology-spring-seminar-{index:02d}-{day}", "ARMP.01.029", "Biology Part 2", day, title))

    biology_lectures = [
        ("2025-03-18", "Lecture: Environment and ecology"),
        ("2025-03-25", "Lecture: Ecology and environment"),
        ("2025-04-01", "Lecture: Evolutionary biology 1"),
        ("2025-04-08", "Lecture: Evolutionary biology 2"),
        ("2025-04-15", "Lecture: Developmental biology 1"),
        ("2025-04-22", "Lecture: Developmental biology 2"),
        ("2025-04-29", "Lecture: Developmental biology 3 and stem cells"),
        ("2025-05-06", "Lecture: Developmental disorders"),
        ("2025-05-13", "Lecture: Ageing"),
        ("2025-05-20", "Lecture: Fertility, infertility and reproductive medicine"),
    ]
    for index, (day, title) in enumerate(biology_lectures, 1):
        events.append(_event(f"biology-spring-lecture-{index:02d}-{day}", "ARMP.01.029", "Biology Part 2", day, title))

    biology_practicals = [
        ("2025-04-22", "Practical: Developmental biology"),
        ("2025-04-29", "Practical: Developmental biology"),
        ("2025-05-06", "Practical: Developmental biology"),
        ("2025-05-09", "Practical: Developmental biology"),
        ("2025-05-20", "Practical: Developmental biology"),
    ]
    for index, (day, title) in enumerate(biology_practicals, 1):
        events.append(_event(f"biology-spring-practical-{index:02d}-{day}", "ARMP.01.029", "Biology Part 2", day, title))

    return sorted(events, key=lambda row: (row["start"], row["uid"]))
