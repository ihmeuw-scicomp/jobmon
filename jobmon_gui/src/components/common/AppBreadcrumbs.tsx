import React from 'react';
import { Link as RouterLink } from 'react-router-dom';
import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import Breadcrumbs from '@mui/material/Breadcrumbs';
import Link from '@mui/material/Link';
import Typography from '@mui/material/Typography';

export interface BreadcrumbItem {
  label: string;
  to?: string;
  active?: boolean;
  onClick?: React.MouseEventHandler<HTMLAnchorElement>;
  onMouseEnter?: React.MouseEventHandler<HTMLAnchorElement>;
}

interface AppBreadcrumbsProps {
  items: BreadcrumbItem[];
  ariaLabel?: string;
}

export function AppBreadcrumbs({
  items,
  ariaLabel = 'breadcrumb',
}: AppBreadcrumbsProps) {
  return (
    <Breadcrumbs
      separator={<NavigateNextIcon fontSize="small" />}
      aria-label={ariaLabel}
    >
      {items.map((item, index) => {
        if (item.active) {
          return (
            <Typography key={index} color="text.primary">
              {item.label}
            </Typography>
          );
        }

        return (
          <Link
            key={index}
            component={RouterLink}
            to={item.to || '#'}
            underline="hover"
            color="inherit"
            onClick={item.onClick}
            onMouseEnter={item.onMouseEnter}
            sx={{
              cursor: 'pointer',
              color: '#007bff',
            }}
          >
            {item.label}
          </Link>
        );
      })}
    </Breadcrumbs>
  );
}
