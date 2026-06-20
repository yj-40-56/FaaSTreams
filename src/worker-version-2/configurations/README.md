| Name | return type | query |
|---|---|---|
| `report_count_per_object` | temporal | Wie oft sich jedes Objekt in dem Fenster gemeldet hat. |
| `presence_span_per_object` | temporal | Erster und letzter Zeitpunkt pro Objekt, plus wie lange es insgesamt da war. |
| `reporting_gap_stats_per_object` | temporal | Wie regelmäßig jedes Objekt meldet – durchschnittliche und größte Lücke zwischen Meldungen. |
| `activity_by_time_bucket` | temporal | Aktivität in 10-Sekunden-Häppchen statt nur einer Zahl fürs ganze Fenster. |
| `window_bounding_extent` | spatial | Der Kartenausschnitt, den alle Positionen zusammen aufspannen. |
| `position_density_grid` | spatial | Wo's voll ist – Positionen in grobe Raster gepackt und gezählt. |
| `per_object_mean_position` | spatial | Durchschnittsposition pro Objekt, falls es mehrmals gemeldet hat. |
| `nearest_object_pairs` | spatial | Wer wem am nächsten ist – jedes Objekt mit seinem nächsten Nachbarn. |
| `ordered_trajectory_points` | spatio_temporal | Der rohe Bewegungspfad jedes Objekts, zeitlich sortiert. |
| `consecutive_step_distance` | spatio_temporal | Strecke und Zeit zwischen zwei aufeinanderfolgenden Meldungen – quasi Geschwindigkeit, ohne dass es ein `speed`-Feld gibt. |
| `total_distance_traveled` | spatio_temporal | Wie weit jedes Objekt insgesamt unterwegs war in dem Fenster. |
| `largest_position_jump` | spatio_temporal | Der größte Sprung pro Objekt – wenn die Geschwindigkeit dabei absurd hoch rauskommt, riecht's nach GPS-Fehler. |