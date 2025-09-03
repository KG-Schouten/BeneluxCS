// import { SmartTable } from './smart-table.js';
// import { CountryFilter, MultiCheckboxFilter } from './filter-widgets.js';
// import { FilterState } from './filter-state.js';
// import { enhanceTableSorting } from './sort-indicators.js';

// let smartTableInstance = null;

document.addEventListener('DOMContentLoaded', () => {
  // Initialize Bootstrap Select
  $('.selectpicker').selectpicker();
  
  // const filterState = new FilterState('statsFilters', {
  //   form: '#stats-table-form',
  //   search: '[name="search"]',
  //   perPage: '[name="per_page"]',
  //   countries: 'input[name="countries"]',
  //   eloSlider: document.querySelector('#elo-slider'),
  //   columns: '[name="columns"]',
  // });

  // filterState.load();

  // smartTableInstance = new SmartTable('#stats-table-wrapper', {
  //   stateKey: 'statsTableState',
  //   dataUrl: '/stats',
  //   formSelector: '#stats-table-form',
  //   tableBoxSelector: '#stats-table-box',
  //   tableBodySelector: '#table-body',
  //   paginationSelector: '#pagination',
  //   scrollableTableSelector: '#scrollable-table',
  //   scrollbarTopSelector: '#scrollbar-top',
  //   perPageSelector: '[name="per_page"]',
  //   searchSelector: '[name="search"]',
  //   initialFilters: {
  //     per_page: 10,
  //     search: '',
  //     page: 1,
  //     scrollPosition: 0,
  //     columns: [],
  //   },
  // });

  // const countryFilter = new CountryFilter({
  //   table: smartTableInstance,
  //   checkboxSelector: 'input[name="countries"]',
  //   name: 'countries',
  // });
  // smartTableInstance.registerFilterWidget('countryFilter', countryFilter);

  // const columnFilter = new MultiCheckboxFilter({
  //   table: smartTableInstance,
  //   checkboxSelector: '.column-checkbox',
  //   filterName: 'columns',
  //   filterState,
  //   resetButtonSelector: '#reset-columns',
  //   toggleInputId: 'toggleVendors',
  //   dropdownWrapperSelector: '.filter-dropdown-wrapper',
  // });
  // columnFilter.init();
  // smartTableInstance.registerFilterWidget('columnFilter', columnFilter);

  // enhanceTableSorting(smartTableInstance, {
  //   indicatorSelector: '.sort-indicator',
  //   ascClass: 'fa-sort-up',
  //   descClass: 'fa-sort-down',
  //   neutralClass: 'fa-sort',
  // });

  // function enableRowHighlighting() {
  //   document.querySelectorAll('.player-row').forEach(row => {
  //     row.addEventListener('mouseenter', () => {
  //       const index = row.dataset.row;
  //       document.querySelectorAll(`.player-row[data-row="${index}"]`).forEach(r => {
  //         r.classList.add('highlight-row');
  //       });
  //     });
  //     row.addEventListener('mouseleave', () => {
  //       const index = row.dataset.row;
  //       document.querySelectorAll(`.player-row[data-row="${index}"]`).forEach(r => {
  //         r.classList.remove('highlight-row');
  //       });
  //     });
  //   });
  // }
  
  // smartTableInstance.onAfterRender(() => {
  //   document.querySelectorAll('.player-row').forEach(row => {
  //     const index = row.dataset.row;
  //     row.addEventListener('mouseenter', () => {
  //       document.querySelectorAll(`.player-row[data-row="${index}"]`)
  //         .forEach(r => r.classList.add('highlight-row'));
  //     });
  //     row.addEventListener('mouseleave', () => {
  //       document.querySelectorAll(`.player-row[data-row="${index}"]`)
  //         .forEach(r => r.classList.remove('highlight-row'));
  //     });
  //   });
  // });

  // console.log('SmartTable initialized', smartTableInstance);
});

document.querySelectorAll('.sidebar-content.accordion-enabled .sidebar-header')
  .forEach(header => {
    header.addEventListener('click', () => {
      const parent = header.parentElement;
      parent.classList.toggle('active');
    });
  });