from flask import Blueprint, render_template, request, redirect, url_for, jsonify
import re

views = Blueprint('views', __name__, template_folder='../templates')

# Redirect root URL to the ESEA page
@views.route('/')
def home_redirect():
    return redirect(url_for('views.esea'))


@views.route('/stats')
def stats_redirect():
    return redirect(url_for('views.stats_player'))

@views.route('/stats/player')
def stats_player():
    from database.db_down_website import gather_filter_options

    try:
        # Gather filter options
        filter_options = gather_filter_options()          
        
        # Full page render
        return render_template(
            "stats/stats_player.html",
            filter_options=filter_options
        )

    except Exception as e:
        # Handle error similarly for both
        error_html = f"""
        <div class="alert alert-danger">
            <h4 class="alert-heading">Error</h4>
            <p>There was an error processing your request: {e}</p>
            <hr>
            <p class="mb-0">Please try refreshing the page.</p>
        </div>
        """

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return error_html

        return render_template(
            "stats/stats_player.html",
            filter_options={},
            error=str(e)
        )

@views.route('/api/stats/player/fields')
def api_stats_player_fields():
    from database.db_down_website import gather_stat_table_columns, gather_columns_mapping
    try:
        stat_field_names = gather_stat_table_columns()
        columns_mapping = gather_columns_mapping()
        
        columns_perm = ['adr', 'k_r_ratio', 'k_d_ratio', 'headshots_percent', 'hltv']
        for col in stat_field_names:
            if col not in columns_mapping:
                columns_mapping[col] = {'name': re.sub(r'^_', '', col).replace('_', ' ').title(), 'round': 2}
        
        return jsonify({
            "stat_field_names": stat_field_names,
            "columns_perm": columns_perm,
            "columns_mapping": columns_mapping
        })
    except Exception as e:
        print(f"ERROR in api_stats route: {e}")
        return jsonify({
            "stat_field_names": [],
            "columns_perm": [],
            "columns_mapping": {},
            "error": str(e)
        })

@views.route('/api/stats/player/data')
def api_stats_player_data():
    from database.db_down_website import gather_player_stats_esea
    
    try:
        # --- Filter Parameter Handling ---
        events = request.args.get('events', '').split(',') if request.args.get('events') else []
        countries = request.args.get('countries', '').split(',') if request.args.get('countries') else []
        season_numbers = request.args.get('seasons', '').split(',') if request.args.get('seasons') else []
        division_names = request.args.get('divisions', '').split(',') if request.args.get('divisions') else []
        stage_names = request.args.get('stages', '').split(',') if request.args.get('stages') else []
        start_date = request.args.get('start_date', "") # UNIX timestamp as string
        end_date = request.args.get('end_date', "") # UNIX timestamp as string
        min_maps_played = request.args.get('min_maps_played', type=int)
        max_maps_played = request.args.get('max_maps_played', type=int)
        team_name = request.args.get('teams_name', [])
        
        data = gather_player_stats_esea(
            events=events,
            countries=countries,
            seasons=season_numbers,
            divisions=division_names,
            stages=stage_names,
            start_date=start_date,
            end_date=end_date,
            min_maps=min_maps_played,
            max_maps=max_maps_played,
            team_name=team_name
        )
        
        return jsonify({
            "data": data
        })
        
    except Exception as e:
        print(f"ERROR in api_stats_player_data route: {e}")
        return jsonify({
            "data": [],
            "error": str(e)
        })


@views.route('/stats/elo')
def stats_elo():
    from database.db_down_website import gather_elo_ranges
    
    try:
        elo_range = gather_elo_ranges()
        
        return render_template(
            'stats/stats_elo.html',
            elo_range=elo_range
        )
    except Exception as e:
        # Handle error similarly for both
        error_html = f"""
        <div class="alert alert-danger">
            <h4 class="alert-heading">Error</h4>
            <p>There was an error processing your request: {e}</p>
            <hr>
            <p class="mb-0">Please try refreshing the page.</p>
        </div>
        """

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return error_html

        return render_template(
            "stats/stats_elo.html",
            filter_options={},
            error=str(e)
        )
    
@views.route('/api/stats/elo/data')
def api_stats_elo_data():
    from database.db_down_website import gather_elo_leaderboard
    
    try:
        countries = request.args.get('countries', '').split(',') if request.args.get('countries') else []
        min_elo = request.args.get('min_elo', type=int)
        max_elo = request.args.get('max_elo', type=int)
        
        data = gather_elo_leaderboard(countries=countries, min_elo=min_elo, max_elo=max_elo)
        
        return jsonify({
            "data": data
        })
    except Exception as e:
        print(f"ERROR in api_stats_elo_data route: {e}")
        return jsonify({
            "data": [],
            "error": str(e)
        })


@views.route('/esea')
def esea():
    # Current time in unix
    from datetime import datetime
    current_time = int(datetime.now().timestamp())
    
    from database.db_down_website import gather_esea_season_info, get_upcoming_matches
    
    # Gathering and separating upcoming matches
    upcoming_matches, end_of_day = get_upcoming_matches()
    
    benelux_matches = [
        match
        for match in upcoming_matches
        if match["team"]["is_benelux"] and match["opponent"]["is_benelux"]
    ]
    
    todays_matches = [
        match
        for match in upcoming_matches
        if match["match_time"] < end_of_day
    ]
    
    # Gather ESEA season info
    season_info = gather_esea_season_info()
    return render_template(
        'esea/esea.html', 
        season_info=season_info, 
        current_time=current_time, 
        end_of_day=end_of_day,
        benelux_matches=benelux_matches, 
        todays_matches=todays_matches
    )

@views.route('/api/esea')
def api_esea():
    from database.db_down_website import gather_columns_mapping
    columns_mapping = gather_columns_mapping()
    return jsonify({
        "columns_mapping": columns_mapping
    })


@views.route('/esea/season/<int:season_number>')
def esea_season_partial(season_number):
    from database.db_down_website import gather_esea_teams_benelux
    esea_data = gather_esea_teams_benelux(szn_number=season_number)
    
    # Extract teams for the specified season
    divisions = esea_data.get(season_number, {})
    return render_template('esea/_esea_season.html', divisions=divisions, season=season_number)