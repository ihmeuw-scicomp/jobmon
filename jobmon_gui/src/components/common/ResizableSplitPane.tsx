import React, { useState, useRef, useCallback, useEffect } from 'react';
import Box from '@mui/material/Box';

interface ResizableSplitPaneProps {
    left: React.ReactNode;
    right: React.ReactNode;
    leftPercent?: number;
    minLeftPercent?: number;
    maxLeftPercent?: number;
    orientation?: 'horizontal' | 'vertical';
}

export default function ResizableSplitPane({
    left,
    right,
    leftPercent: controlledPercent = 60,
    minLeftPercent = 20,
    maxLeftPercent = 80,
    orientation = 'horizontal',
}: ResizableSplitPaneProps) {
    const isVertical = orientation === 'vertical';
    const [localPercent, setLocalPercent] = useState(controlledPercent);
    const userDragged = useRef(false);
    const containerRef = useRef<HTMLDivElement>(null);
    const dragging = useRef(false);

    // Sync from parent when user hasn't manually dragged
    useEffect(() => {
        if (!userDragged.current) {
            setLocalPercent(controlledPercent);
        }
    }, [controlledPercent]);

    const handlePointerDown = useCallback(
        (e: React.PointerEvent) => {
            e.preventDefault();
            dragging.current = true;
            userDragged.current = true;
            (e.target as HTMLElement).setPointerCapture(e.pointerId);
        },
        []
    );

    const handlePointerMove = useCallback(
        (e: React.PointerEvent) => {
            if (!dragging.current || !containerRef.current) return;
            const rect = containerRef.current.getBoundingClientRect();
            const pct = isVertical
                ? ((e.clientY - rect.top) / rect.height) * 100
                : ((e.clientX - rect.left) / rect.width) * 100;
            setLocalPercent(
                Math.min(maxLeftPercent, Math.max(minLeftPercent, pct))
            );
        },
        [isVertical, minLeftPercent, maxLeftPercent]
    );

    const handlePointerUp = useCallback(() => {
        dragging.current = false;
    }, []);

    // Prevent text selection while dragging
    useEffect(() => {
        const onSelectStart = (e: Event) => {
            if (dragging.current) e.preventDefault();
        };
        document.addEventListener('selectstart', onSelectStart);
        return () =>
            document.removeEventListener('selectstart', onSelectStart);
    }, []);

    const firstPanelSx = isVertical
        ? {
              flex: `0 0 ${localPercent}%`,
              maxHeight: `${localPercent}%`,
              minHeight: 0,
              overflow: 'hidden',
              display: 'flex',
          }
        : {
              flex: `0 0 ${localPercent}%`,
              maxWidth: `${localPercent}%`,
              minWidth: 0,
              overflow: 'hidden',
          };

    const handleHitTarget = isVertical
        ? {
              content: '""',
              position: 'absolute',
              left: 0,
              right: 0,
              top: -4,
              bottom: -4,
          }
        : {
              content: '""',
              position: 'absolute',
              top: 0,
              bottom: 0,
              left: -4,
              right: -4,
          };

    const secondPanelSx = isVertical
        ? { flex: 1, minHeight: 0, overflow: 'hidden' }
        : { flex: 1, minWidth: 0, overflow: 'hidden' };

    return (
        <Box
            ref={containerRef}
            sx={{
                display: 'flex',
                flexDirection: isVertical ? 'column' : 'row',
                flex: 1,
                minHeight: 0,
                overflow: 'hidden',
            }}
        >
            {/* First panel */}
            <Box sx={firstPanelSx}>
                {left}
            </Box>

            {/* Drag handle */}
            <Box
                onPointerDown={handlePointerDown}
                onPointerMove={handlePointerMove}
                onPointerUp={handlePointerUp}
                sx={{
                    flex: '0 0 5px',
                    cursor: isVertical ? 'row-resize' : 'col-resize',
                    backgroundColor: 'divider',
                    position: 'relative',
                    zIndex: 1,
                    '&:hover, &:active': {
                        backgroundColor: 'primary.main',
                    },
                    '&::before': handleHitTarget,
                }}
            />

            {/* Second panel */}
            <Box sx={secondPanelSx}>
                {right}
            </Box>
        </Box>
    );
}
