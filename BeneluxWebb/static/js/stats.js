import { SmartTable } from './smart-table.js';
import { CountryFilter, TomSelectFilter, MultiCheckboxFilter } from './filter-widgets.js';
import { FilterState } from './filter-state.js';
import { enhanceTableSorting } from './sort-indicators.js';

let smartTableInstance = null;

document.addEventListener('DOMContentLoaded', () => {
  const filterState = new FilterState('statsFilters', {
    form: '#stats-table-form',
    search: '[name="search"]',
    perPage: '[name="per_page"]',
    countries: 'input[name="countries"]',
    eloSlider: document.querySelector('#elo-slider'),
    columns: '[name="columns"]',
  });

  filterState.load();

  smartTableInstance = new SmartTable('#stats-table-wrapper', {
    stateKey: 'statsTableState',
    dataUrl: '/stats',
    formSelector: '#stats-table-form',
    tableBoxSelector: '#stats-table-box',
    tableBodySelector: '#table-body',
    paginationSelector: '#pagination',
    scrollableTableSelector: '#scrollable-table',
    scrollbarTopSelector: '#scrollbar-top',
    perPageSelector: '[name="per_page"]',
    searchSelector: '[name="search"]',
    initialFilters: {
      per_page: 10,
      search: '',
      page: 1,
      scrollPosition: 0,
      columns: [],
    },
  });

  const countryFilter = new CountryFilter({
    table: smartTableInstance,
    checkboxSelector: 'input[name="countries"]',
    name: 'countries',
  });
  smartTableInstance.registerFilterWidget('countryFilter', countryFilter);

  const seasonFilter = new TomSelectFilter({
    selector: '#season-select',
    table: smartTableInstance,
    filterName: 'seasons',
    filterState,
  });
  seasonFilter.init();
  smartTableInstance.registerFilterWidget('seasonFilter', seasonFilter);

  const divisionFilter = new TomSelectFilter({
    selector: '#division-select',
    table: smartTableInstance,
    filterName: 'divisions',
    filterState,
  });
  divisionFilter.init();
  smartTableInstance.registerFilterWidget('divisionFilter', divisionFilter);

  const stageFilter = new TomSelectFilter({
    selector: '#stage-select',
    table: smartTableInstance,
    filterName: 'stages',
    filterState,
  });
  stageFilter.init();
  smartTableInstance.registerFilterWidget('stageFilter', stageFilter);

  const columnFilter = new MultiCheckboxFilter({
    table: smartTableInstance,
    checkboxSelector: '.column-checkbox',
    filterName: 'columns',
    filterState,
    resetButtonSelector: '#reset-columns',
    toggleInputId: 'toggleVendors',
    dropdownWrapperSelector: '.filter-dropdown-wrapper',
  });
  columnFilter.init();
  smartTableInstance.registerFilterWidget('columnFilter', columnFilter);

  enhanceTableSorting(smartTableInstance, {
    indicatorSelector: '.sort-indicator',
    ascClass: 'fa-sort-up',
    descClass: 'fa-sort-down',
    neutralClass: 'fa-sort',
  });

  function enableRowHighlighting() {
    document.querySelectorAll('.player-row').forEach(row => {
      row.addEventListener('mouseenter', () => {
        const index = row.dataset.row;
        document.querySelectorAll(`.player-row[data-row="${index}"]`).forEach(r => {
          r.classList.add('highlight-row');
        });
      });
      row.addEventListener('mouseleave', () => {
        const index = row.dataset.row;
        document.querySelectorAll(`.player-row[data-row="${index}"]`).forEach(r => {
          r.classList.remove('highlight-row');
        });
      });
    });
  }
  
  smartTableInstance.onAfterRender(() => {
    document.querySelectorAll('.player-row').forEach(row => {
      const index = row.dataset.row;
      row.addEventListener('mouseenter', () => {
        document.querySelectorAll(`.player-row[data-row="${index}"]`)
          .forEach(r => r.classList.add('highlight-row'));
      });
      row.addEventListener('mouseleave', () => {
        document.querySelectorAll(`.player-row[data-row="${index}"]`)
          .forEach(r => r.classList.remove('highlight-row'));
      });
    });
  });

  console.log('SmartTable initialized', smartTableInstance);
});