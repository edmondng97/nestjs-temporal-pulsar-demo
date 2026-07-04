// demo/scroll.js — scroll-driven deck: hero word entrance, pinned scrub scenes
// (architecture chain, epoch fence), per-slide reveals and the progress bar.
(() => {
  gsap.registerPlugin(ScrollTrigger);
  gsap.defaults({ ease: 'power3.out', duration: 0.8 });

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

  // Wrap each word of the hero title in a span so words can flip in individually.
  function splitWords(el) {
    // extraClass: background-clip gradients don't paint through inline-block
    // children, so the gradient class moves onto each word span itself.
    const wrap = (node, extraClass) => {
      const frag = document.createDocumentFragment();
      node.textContent.split(/(\s+)/).forEach((part) => {
        if (!part) return;
        if (/^\s+$/.test(part)) { frag.appendChild(document.createTextNode(part)); return; }
        const span = document.createElement('span');
        span.className = extraClass ? `hero-word ${extraClass}` : 'hero-word';
        span.textContent = part;
        frag.appendChild(span);
      });
      node.replaceWith(frag);
    };
    [...el.childNodes].forEach((n) => {
      if (n.nodeType === Node.TEXT_NODE) wrap(n, '');
      else if (n.nodeType === Node.ELEMENT_NODE && n.tagName !== 'BR') {
        const cls = n.classList.contains('gradient') ? 'gradient' : '';
        [...n.childNodes].forEach((c) => { if (c.nodeType === Node.TEXT_NODE) wrap(c, cls); });
      }
    });
  }

  // If any animation setup throws, reveal everything so content is never lost.
  function safeInit(fn) {
    try { fn(); } catch (err) {
      console.error('demo init failed, revealing content:', err);
      gsap.set('.slide, .slide *', { clearProps: 'opacity,visibility,transform' });
    }
  }

  const mm = gsap.matchMedia();

  mm.add(
    {
      motionOK: '(prefers-reduced-motion: no-preference)',
      isDesktop: '(min-width: 900px)',
    },
    (ctx) => {
      const { motionOK, isDesktop } = ctx.conditions;
      if (!motionOK) return; // reduced motion: no tweens, content simply visible
      const pinScenes = isDesktop; // pinned scrub scenes only on wide screens

      const hero = slides[0];
      const archSlide = slides.find((s) => s.querySelector('.arch'));
      const fenceSlide = slides.find((s) => s.querySelector('.fence'));

      // ---- Hero: word-by-word 3D entrance, then parallax exit tied to scroll.
      safeInit(() => {
        splitWords(hero.querySelector('h1'));
        gsap.timeline()
          .from(hero.querySelector('.kicker'), { y: 20, autoAlpha: 0, duration: 0.5 })
          .from(hero.querySelectorAll('.hero-word'), {
            y: 60, autoAlpha: 0, rotationX: -40, stagger: 0.05,
            duration: 0.7, ease: 'back.out(1.6)',
          }, '-=0.2')
          .from(hero.querySelectorAll('.sub, .glow-card'), {
            y: 24, autoAlpha: 0, stagger: 0.06, duration: 0.6,
          }, '-=0.3');

        gsap.to(hero.children, {
          y: -120, autoAlpha: 0, ease: 'none',
          scrollTrigger: { trigger: hero, start: 'top top', end: 'bottom 35%', scrub: true },
        });
      });

      // ---- Kickers: letter-spacing settles in as each section arrives.
      safeInit(() => {
        slides.slice(1).forEach((slide) => {
          const k = slide.querySelector('.kicker');
          if (k) gsap.from(k, {
            autoAlpha: 0, letterSpacing: '0.6em', duration: 0.8,
            scrollTrigger: { trigger: slide, start: 'top 85%' },
          });
        });
      });

      // ---- Generic reveals: glow-card grids pop per card, steps slide in per row.
      safeInit(() => {
        slides.slice(1).forEach((slide) => {
          const targets = [];
          slide.querySelectorAll('.rise:not(li):not(.kicker)').forEach((r) => {
            const cards = r.querySelectorAll(':scope > .glow-card');
            if (cards.length > 1) targets.push(...cards);
            else targets.push(r);
          });
          if (targets.length) {
            gsap.from(targets, {
              y: 28, autoAlpha: 0, scale: 0.97, duration: 0.7,
              stagger: 0.12, ease: 'back.out(1.2)',
              scrollTrigger: {
                trigger: slide, start: 'top 65%',
                toggleActions: 'restart none none reverse',
              },
            });
          }
          const steps = slide.querySelectorAll('.steps li');
          if (steps.length) {
            gsap.from(steps, {
              x: -48, autoAlpha: 0, duration: 0.6, stagger: 0.12,
              scrollTrigger: {
                trigger: slide, start: 'top 60%',
                toggleActions: 'restart none none reverse',
              },
            });
          }
        });
      });

      // ---- Architecture: pinned scene — nodes light up left to right as you scroll.
      safeInit(() => {
        if (!archSlide) return;
        const nodes = archSlide.querySelectorAll('.arch .node');
        const arrows = archSlide.querySelectorAll('.arch .arrow');
        if (!nodes.length) return;

        if (!pinScenes) {
          gsap.from([...nodes].flatMap((n, i) => (i ? [arrows[i - 1], n] : [n])), {
            autoAlpha: 0, y: 24, stagger: 0.15, duration: 0.5,
            scrollTrigger: { trigger: archSlide, start: 'top 60%' },
          });
          return;
        }

        const tl = gsap.timeline({
          scrollTrigger: {
            trigger: archSlide, start: 'top top', end: '+=1400',
            pin: true, scrub: 0.6,
            onUpdate(self) {
              const lit = Math.floor(self.progress * nodes.length + 0.0001);
              nodes.forEach((n, i) => n.classList.toggle('lit', i < lit));
            },
          },
        });
        nodes.forEach((n, i) => {
          if (i) tl.from(arrows[i - 1], { autoAlpha: 0, x: -12, duration: 0.4 });
          tl.from(n, { autoAlpha: 0, y: 30, scale: 0.9, duration: 0.6 });
        });
        tl.to({}, { duration: 0.5 }); // hold beat before unpin
      });

      // ---- Epoch fence: pinned choreography — stale charges the wall, the wall
      // recoils, stale bounces off dimmed; fresh passes through with a glow pulse.
      safeInit(() => {
        if (!fenceSlide) return;
        const stale = fenceSlide.querySelector('.msg.stale');
        const fresh = fenceSlide.querySelector('.msg.fresh');
        const wall = fenceSlide.querySelector('.wall');
        if (!stale || !fresh || !wall) return;

        const st = pinScenes
          ? { trigger: fenceSlide, start: 'top top', end: '+=1600', pin: true, scrub: 0.6 }
          : { trigger: fenceSlide, start: 'top 55%', toggleActions: 'restart none none reverse' };

        gsap.timeline({ defaults: { ease: 'power2.out' }, scrollTrigger: st })
          .from(stale, { autoAlpha: 0, x: -30, duration: 0.5 })
          .to(stale, { x: 46, duration: 0.7, ease: 'power1.in' }, '+=0.3')
          .to(wall, { keyframes: [{ x: -6 }, { x: 4 }, { x: 0 }], duration: 0.3 }, '<0.6')
          .to(stale, { x: -14, duration: 0.35 })
          .to(stale, { x: -8, autoAlpha: 0.45, duration: 0.45 }, '<0.1')
          .to(fresh, { x: -120, autoAlpha: 0.9, duration: 0.9, ease: 'power2.inOut' }, '-=0.5')
          .to(fresh, {
            boxShadow: '0 0 26px rgba(74,222,128,.6)', duration: 0.3,
            yoyo: true, repeat: 1,
          })
          .to({}, { duration: 0.4 }); // hold beat before unpin
      });

      // ---- Slide exits: content drifts up and fades as each section leaves.
      safeInit(() => {
        slides.forEach((slide, i) => {
          if (i === slides.length - 1) return;
          if (slide === hero || (pinScenes && (slide === archSlide || slide === fenceSlide))) return;
          gsap.to(slide.children, {
            y: -60, autoAlpha: 0, ease: 'none',
            scrollTrigger: { trigger: slide, start: 'bottom 40%', end: 'bottom top', scrub: true },
          });
        });
      });
    }
  );
})();
