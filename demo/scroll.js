// demo/scroll.js — scroll-driven deck: GSAP ScrollTrigger reveals each section
// on entry (replayed on re-entry) and drives the bottom progress bar.
(() => {
  gsap.registerPlugin(ScrollTrigger);

  const slides = gsap.utils.toArray('.slide');
  const progress = document.getElementById('progress');
  const segments = slides.map(() => {
    const s = document.createElement('div');
    s.className = 'seg';
    progress.appendChild(s);
    return s;
  });

  // Progress bar: segment j lights up while section j (or a later one) is current.
  slides.forEach((slide, j) => {
    ScrollTrigger.create({
      trigger: slide,
      start: 'top center',
      end: 'bottom center',
      onToggle: (self) => {
        if (!self.isActive) return;
        segments.forEach((seg, k) => seg.classList.toggle('done', k <= j));
      },
    });
  });

  const mm = gsap.matchMedia();

  mm.add('(prefers-reduced-motion: no-preference)', () => {
    slides.forEach((slide) => {
      const rises = slide.querySelectorAll('.rise');
      if (rises.length) {
        gsap.from(rises, {
          y: 18,
          autoAlpha: 0,
          duration: 0.6,
          ease: 'power2.out',
          stagger: 0.15,
          scrollTrigger: {
            trigger: slide,
            start: 'top 65%',
            // Replay on every re-entry, reverse out when scrolled back above.
            toggleActions: 'restart none none reverse',
          },
        });
      }

      // Flagship epoch-fence choreography: the stale message charges the wall
      // and bounces off dimmed; the fresh one passes through.
      const stale = slide.querySelector('.msg.stale');
      const fresh = slide.querySelector('.msg.fresh');
      if (stale && fresh) {
        gsap
          .timeline({
            defaults: { ease: 'power2.out' },
            scrollTrigger: {
              trigger: slide,
              start: 'top 55%',
              toggleActions: 'restart none none reverse',
            },
          })
          .to(stale, { x: 46, duration: 0.7, ease: 'power1.in', delay: 0.8 })
          .to(stale, { x: -14, duration: 0.35 })
          .to(stale, { x: -8, autoAlpha: 0.45, duration: 0.45 }, '<0.1')
          .to(fresh, { x: -120, autoAlpha: 0.9, duration: 0.9, ease: 'power2.inOut' }, '-=0.5');
      }
    });
  });
  // Reduced motion: no tweens are created, content is simply visible.
})();
