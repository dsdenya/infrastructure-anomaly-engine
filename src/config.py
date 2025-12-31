US_STATE_TO_ABBREV= {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
    "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN",
    "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC",
    "Puerto Rico": "PR"
}

UNKNOWN_CAUSES_LIST = [
    'Distribution Interruption - Unknown Cause',
    'Suspected Cyber Attack',
    'Sabotage',
    'Suspicious Activity ',
    'Other',
    'Cyber Event',
    'Transmission Interruption',
    'Suspicious Activity',
    'Distribution Interruption ',
    'Transmission Disruption',
    'Suspicious activity',
    '- Other',
    '- Unknown',
    '- Suspicious activity',
    '- Transmission equipment failure',
    '- Unknown\xa0- Failure at high voltage substation or switchyard'
]

VALID_CODES = set(US_STATE_TO_ABBREV.values())
