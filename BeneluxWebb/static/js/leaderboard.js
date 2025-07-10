import { SmartTable } from "./smart-table.js";
import { RangeSlider, CountryFilter } from "./filter-widgets.js";
import { FilterState } from "./filter-state.js";
import { enhanceTableSorting } from "./sort-indicators.js";

let smartTableInstance = null;

document.addEventListener("DOMContentLoaded", () => {
  // Step 1: Set up persistent filter state
  const filterState = new FilterState("leaderboardFilters", {
    form: "#leaderboard-table-form",
    search: '[name="search"]',
    perPage: '[name="per_page"]',
    countries: 'input[name="countries"]',
    eloSlider: '#elo-slider',
  });

  filterState.load(); // Step 2: Load saved state before table setup

  // Step 3: Set up SmartTable instance
  const leaderboardTable = new SmartTable("#leaderboard-wrapper", {
    stateKey: "leaderboardTableState",
    dataUrl: "/leaderboard",

    formSelector: "#leaderboard-table-form",
    tableBoxSelector: ".leaderboard-table-box",
    tableBodySelector: ".leaderboard-body",
    paginationSelector: "#pagination",
    scrollableTableSelector: ".scrollable-table",
    scrollbarTopSelector: ".scrollbar-top",
    perPageSelector: '[name="per_page"]',
    searchSelector: '[name="search"]',

    initialFilters: {
      per_page: 20,
      search: "",
      page: 1,
      scrollPosition: 0,
      min_elo: 0,
      max_elo: 5000,
    },
  });

  smartTableInstance = leaderboardTable;
  leaderboardTable.refresh = () => leaderboardTable.fetchData();
  
  // Step 4: Set up filters with filterState
  const countryFilter = new CountryFilter({
    table: leaderboardTable,
    checkboxSelector: 'input[name="countries"]',
    name: 'countries',
    filterState, // persist country selections
  });
  leaderboardTable.registerFilterWidget('countryFilter', countryFilter);

  const eloSlider = new RangeSlider({
    table: leaderboardTable,
    container: document.getElementById("elo-slider"),
    minName: 'min_elo',
    maxName: 'max_elo',
    minValue: 0,
    maxValue: 5000,
    step: 50,
    minInput: document.getElementById("min-elo"),
    maxInput: document.getElementById("max-elo"),
    minDisplay: document.getElementById("elo-min-value"),
    maxDisplay: document.getElementById("elo-max-value"),
    filterState, // persist ELO slider range
  });
  leaderboardTable.registerFilterWidget('eloSlider', eloSlider);

  // Step 5: Enhance table sorting
  enhanceTableSorting(leaderboardTable, {
    indicatorSelector: '.sort-indicator',
    ascClass: 'fa-sort-up',
    descClass: 'fa-sort-down',
    neutralClass: 'fa-sort'
  });
});
