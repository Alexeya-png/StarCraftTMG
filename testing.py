import random
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if (ROOT / 'app').exists():
    sys.path.insert(0, str(ROOT))
else:
    sys.path.insert(0, str(ROOT.parent))

from app.database import submit_match_result  # noqa: E402

RNG = random.Random(1337)
RACES = ('Терран', 'Протосс', 'Зерг')
GAME_TYPES = ('1к', '2к', 'Grand Offensive')
MISSIONS = (
    'Agria Valley',
    'Backwater Station',
    'Char Frontier',
    'Korhal Plateau',
    'Mar Sara Dustlands',
    'Shakuras Ridge',
    'Tarsonis Ruins',
    'XelNaga Outpost',
)
COMMENTS = (
    '',
    '',
    '',
    'Close macro game.',
    'Fast timing attack decided the match.',
    'Long positional game with counterattacks.',
    'Aggressive opener into midgame pressure.',
    'Late game comeback.',
)

PLAYERS = [
    {'name': 'Raynor', 'main_race': 'Терран'},
    {'name': 'Tychus', 'main_race': 'Терран'},
    {'name': 'Nova', 'main_race': 'Терран'},
    {'name': 'Swann', 'main_race': 'Терран'},
    {'name': 'Marauder', 'main_race': 'Терран'},
    {'name': 'SiegeOne', 'main_race': 'Терран'},
    {'name': 'Fenix', 'main_race': 'Протосс'},
    {'name': 'Artanis', 'main_race': 'Протосс'},
    {'name': 'Tassadar', 'main_race': 'Протосс'},
    {'name': 'Zeratul', 'main_race': 'Протосс'},
    {'name': 'Karax', 'main_race': 'Протосс'},
    {'name': 'Aldaris', 'main_race': 'Протосс'},
    {'name': 'Kerrigan', 'main_race': 'Зерг'},
    {'name': 'Zagara', 'main_race': 'Зерг'},
    {'name': 'Stukov', 'main_race': 'Зерг'},
    {'name': 'Abathur', 'main_race': 'Зерг'},
    {'name': 'Dehaka', 'main_race': 'Зерг'},
    {'name': 'Overmind', 'main_race': 'Зерг'},
    {'name': 'GhostWolf', 'main_race': 'Терран'},
    {'name': 'DragoonAce', 'main_race': 'Протосс'},
    {'name': 'HydraKing', 'main_race': 'Зерг'},
    {'name': 'Battlecruiser', 'main_race': 'Терран'},
    {'name': 'CarrierCore', 'main_race': 'Протосс'},
    {'name': 'MutaStorm', 'main_race': 'Зерг'},
]


def pick_race(main_race: str) -> str:
    pool = [main_race] * 7 + [race for race in RACES if race != main_race]
    return RNG.choice(pool)


def pick_game_type() -> str:
    roll = RNG.random()
    if roll < 0.78:
        return '1к'
    if roll < 0.94:
        return '2к'
    return 'Grand Offensive'


def build_matches(total_matches: int = 300):
    base_time = datetime.now() - timedelta(days=120)
    pair_counter: Counter[tuple[str, str]] = Counter()
    generated: list[dict] = []

    for index in range(total_matches):
        player1, player2 = RNG.sample(PLAYERS, 2)

        ordered_pair = tuple(sorted((player1['name'], player2['name'])))
        if pair_counter[ordered_pair] >= 6:
            attempts = 0
            while pair_counter[ordered_pair] >= 6 and attempts < 50:
                player1, player2 = RNG.sample(PLAYERS, 2)
                ordered_pair = tuple(sorted((player1['name'], player2['name'])))
                attempts += 1

        pair_counter[ordered_pair] += 1

        p1_race = pick_race(player1['main_race'])
        p2_race = pick_race(player2['main_race'])
        winner = RNG.choice((player1, player2))

        generated.append(
            {
                'winner_name': winner['name'],
                'opponent_name': player2['name'] if winner['name'] == player1['name'] else player1['name'],
                'winner_race': p1_race if winner['name'] == player1['name'] else p2_race,
                'opponent_race': p2_race if winner['name'] == player1['name'] else p1_race,
                'is_ranked': True,
                'game_type': pick_game_type(),
                'mission_name': RNG.choice(MISSIONS),
                'comment': RNG.choice(COMMENTS),
                'played_at': base_time + timedelta(hours=index * 9 + RNG.randint(0, 3)),
            }
        )

    generated.sort(key=lambda item: item['played_at'])
    return generated


def main() -> None:
    matches = build_matches(300)

    inserted = 0
    for match in matches:
        submit_match_result(
            winner_name=match['winner_name'],
            opponent_name=match['opponent_name'],
            winner_race=match['winner_race'],
            opponent_race=match['opponent_race'],
            is_ranked=match['is_ranked'],
            game_type=match['game_type'],
            mission_name=match['mission_name'],
            comment=match['comment'],
        )
        inserted += 1

    print(f'Inserted {inserted} test matches between {len(PLAYERS)} players.')
    print('Players:')
    for player in PLAYERS:
        print(f"- {player['name']} ({player['main_race']})")


if __name__ == '__main__':
    main()
