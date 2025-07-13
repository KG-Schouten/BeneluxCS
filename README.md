# Benelux Counter-Strike 🇧🇪🇳🇱🇱🇺
A comprehensive platform for tracking the Counter-Strike scene in the BeNeLux.

Want to be a part of the BeNeLux CS Community? [Join the Discord!](https://discord.gg/9xzsGTGhjm)

## Current Status & To-Do (BeneluxCS.nl)
### Currently Known Issues
- Leaderboard: Country validation not working on the leaderboard. Expect fake-flaggers
- Leaderboard: Elo slider filtering unresponsive with fast movements
- Narrow layout (phones etc.) not working. Breaks the website layout lol
- Stats: At first load, country filters show active even though no filters are applied.
### Upcoming Features/Projects
- Benelux Hub integration! Matches, Stats, Leaderboards and the rest :D
- Leaderboard enhancements: Configure slider min/max to match leaderboard elo range
- ESEA: Add detailed player statistics to team pages
- Stats: Add additional filters (Maps played, Map-specific filtering, Team filtering)
- Stats/Leaderboard: Add Team label when player participates in ESEA
- An info hub for casters / observers / hosts in the BeNeLux.
- An Events page containing info about past and upcoming BeNeLux CS events (lans, tourneys, comps)
- An stylistic overhaul of the website (desperately needed lol)

## Project Overview
BeneluxCS provides a centralized hub for Counter-Strike statistics focused on the Benelux region. The platform offers:
- **ESEA Tracking**: Complete coverage of ESEA seasons, teams and matches in the BeNeLux.
- **Player Leaderboard**: ELO Leaderboard for all BeNeLux faceit players. (including fake-flag detection)
- **Statistics**: Match statistics table for all ESEA matches in the BeNeLux

## Technical Architecture
### Frontend (BeneluxWebb)
Web interface made with Flask, HTML and JS providing a means to show off the data gathered from the FACEIT API.

### Backend (data_processing)
All functions and files connected to gathering and processing the data from the FACEIT (and in beta STEAM) API's.

### Database
The SQLite relational database storing data on various events and leagues on faceit:
- Players and stats
- Teams and events played
- Match results and statistics
- ESEA season information
- Event information

## License
This project is licensed under the GNU General Public License v3.0

## Contact
for questions or feedback:
- [Discord](https://discordapp.com/users/228206535405207552)
- [Steam](https://steamcommunity.com/id/Fowlz1/)
- [Twitter](https://steamcommunity.com/id/Fowlz1/)