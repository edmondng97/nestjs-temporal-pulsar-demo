// demo/deck.js — minimal keynote engine: keyboard/click nav + progress bar.
(() => {
  const slides = Array.from(document.querySelectorAll('.slide'));
  const progress = document.getElementById('progress');
  let index = 0;

  const segments = slides.map(() => {
    const s = document.createElement('div');
    s.className = 'seg';
    progress.appendChild(s);
    return s;
  });

  function show(i) {
    index = Math.max(0, Math.min(slides.length - 1, i));
    slides.forEach((sl, j) => {
      sl.classList.toggle('active', j === index);
      // 'entered' triggers per-slide CSS animations; removed when leaving so
      // re-visiting a slide replays its animation.
      if (j === index) requestAnimationFrame(() => sl.classList.add('entered'));
      else sl.classList.remove('entered');
      segments[j].classList.toggle('done', j <= index);
    });
  }

  document.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowRight' || e.key === ' ' || e.key === 'PageDown') show(index + 1);
    if (e.key === 'ArrowLeft' || e.key === 'PageUp') show(index - 1);
    if (e.key === 'Home') show(0);
    if (e.key === 'End') show(slides.length - 1);
  });
  document.addEventListener('click', (e) => {
    if (e.target.closest('a, code, pre')) return; // don't hijack link/code clicks
    show(e.clientX > window.innerWidth / 2 ? index + 1 : index - 1);
  });

  show(0);
})();
