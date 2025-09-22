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

@views.route('/api/stats/fields')
def api_stats():
    from database.db_down_website import gather_stat_table_columns
    try:
        stat_field_names = gather_stat_table_columns()
        
        columns_perm = ['adr', 'k_r_ratio', 'k_d_ratio', 'headshots_percent', 'hltv']
        columns_mapping = {
            'adr':                  {'name': 'ADR',         'round': 0},
            'k_r_ratio':            {'name': 'K/R',         'round': 2},
            'k_d_ratio':            {'name': 'K/D',         'round': 2, 'good': 1.05, 'bad': 0.95},
            'headshots_percent':    {'name': 'HS %',        'round': 0},
            'hltv':                 {'name': 'HLTV 1.0',    'round': 2, 'good': 1.05, 'bad': 0.95},
        }
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

@views.route('/api/stats/data')
def api_stats_data():
    from database.db_down_website import gather_player_stats_esea
    
    try:
        # --- Filter Parameter Handling ---
        events = request.args.get('events', '').split(',') if request.args.get('events') else []
        countries = request.args.get('countries', '').split(',') if request.args.get('countries') else []
        season_numbers = request.args.get('seasons', '').split(',') if request.args.get('seasons') else []
        division_names = request.args.get('divisions', '').split(',') if request.args.get('divisions') else []
        stage_names = request.args.get('stages', '').split(',') if request.args.get('stages') else []
        start_date = request.args.get('start_date') # YYYY-MM-DD
        end_date = request.args.get('end_date')     # YYYY-MM-DD
        min_maps = request.args.get('min_maps_played', type=int)
        max_maps = request.args.get('max_maps_played', type=int)
        team_ids = request.args.get('teams_ids', [])
        
        data = gather_player_stats_esea(
            events=events,
            countries=countries,
            seasons=season_numbers,
            divisions=division_names,
            stages=stage_names,
            start_date=start_date,
            end_date=end_date,
            min_maps=min_maps,
            max_maps=max_maps,
            team_ids=team_ids
        )
        
        return jsonify({
            "data": data
        })
        
    except Exception as e:
        print(f"ERROR in api_stats_data route: {e}")
        return jsonify({
            "data": [],
            "error": str(e)
        })

@views.route('/stats/elo')
def stats_elo():
    return render_template(
        'stats/stats_elo.html'
    )


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

@views.route('/esea/season/<int:season_number>')
def esea_season_partial(season_number):
    from database.db_down_website import gather_esea_teams_benelux
    esea_data = gather_esea_teams_benelux(szn_number=season_number)
    
    # Extract teams for the specified season
    divisions = esea_data.get(season_number, {})
    return render_template('esea/_esea_season.html', divisions=divisions, season=season_number)

@views.route('/leaderboard')
def leaderboard():
    # Parse parameters with robust error handling
    try:
        # Country parameter handling
        countries_str = request.args.get('countries')
        countries = countries_str.split(',') if countries_str else []
        if not countries:
            countries = ['nl', 'be', 'lu']  # Default to Benelux countries
            print("No countries provided, defaulting to all:", countries)
        else:
            print("Countries from request:", countries)
        
        search = request.args.get('search', '').strip()
        min_elo = request.args.get('min_elo', type=int, default=0)
        max_elo = request.args.get('max_elo', type=int, default=5000)
        page = max(1, request.args.get('page', type=int, default=1))  # Ensure page is at least 1
        per_page = request.args.get('per_page', type=int, default=20)
        
        # Sort parameter handling
        sort = request.args.get('sort')
        sort_key = None
        sort_desc = False
        if sort:
            if sort.startswith('-'):
                sort_desc = True
                sort_key = sort[1:]
            else:
                sort_key = sort
                
        # Ensure per_page is within reasonable bounds
        per_page = max(10, min(per_page, 100))
            
        # Get data from the database
        from database.db_down_website import gather_leaderboard
        data = gather_leaderboard(
            countries=countries,
            search=search,
            min_elo=min_elo,
            max_elo=max_elo,
        )
        
        # Sort data
        sort_mapping = {
            'elo': 'faceit_elo',
        }
        if sort_key:
            try:
                data.sort(key=lambda x: x.get(sort_mapping[sort_key], 0), reverse=sort_desc)
            except (KeyError, TypeError):
                pass  # If sorting fails, continue with unsorted data
        
        # Pagination
        total = len(data)
        start = (page - 1) * per_page
        end = start + per_page
        paginated_data = data[start:end]
        total_pages = ((total - 1) // per_page) + 1 if total > 0 else 1

        # Check if this is an AJAX request
        is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        
        if is_ajax:
            return render_template(
                "leaderboard/_leaderboard_table.html",
                data=paginated_data,
                sort=sort,
                page=page,
                per_page=per_page,
                total=total,
                total_pages=total_pages
            )
        
        return render_template(
            "leaderboard/leaderboard.html",
            data=paginated_data,
            search=search,
            min_elo=min_elo,
            max_elo=max_elo,
            sort=sort,
            page=page,
            total=total,
            per_page=per_page,
            total_pages=total_pages
        )
    
    except Exception as e:
        print(f"ERROR in leaderboard route: {e}")
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return f"""
            <div class="alert alert-danger">
                <h4 class="alert-heading">Error</h4>
                <p>There was an error processing your request: {e}</p>
                <hr>
                <p class="mb-0">Please try refreshing the page.</p>
            </div>
            """
        
        return render_template(
            "leaderboard/leaderboard.html", 
            data=[],
            search='',
            min_elo=0,
            max_elo=5000,
            sort='elo_desc',
            page=1,
            total=0,
            per_page=20,
            total_pages=1,
            error=str(e)
        )