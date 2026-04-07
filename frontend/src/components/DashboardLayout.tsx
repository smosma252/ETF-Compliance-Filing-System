import React, { useState } from "react";
import { cn } from '../lib/utils';
import { Link, useLocation } from "wouter";
import {toast} from "sonner"
import { Badge } from '../components/Badge';
import {
  LayoutDashboard,
  Building2,
  BarChart3,
  AlertTriangle,
  Upload,
  History,
  Shield
} from 'lucide-react';


interface NavItems {
    label: string
    href: string
    icon: React.ElementType
    badge?: number
}
const NavItems: NavItems[] = [
    {label: "Dashboard", href: "/", icon: LayoutDashboard},
    {label: "Funds", href: "/Funds", icon: Building2},
    {label: "Holdings", href: "/Holdings", icon: BarChart3},
    {label: "Exceptions", href: "/Exceptions", icon: AlertTriangle},
    {label: "Upload", href: "/Upload", icon: Upload},
    {label: "Audit", href: "/Audit", icon: History},
]

interface DashboardLayoutProps {
    children: React.ReactNode 
    title?: string
    subtitle?: string
    actions?: React.ReactNode
}


function DashboardLayout({children, title, subtitle, actions}: DashboardLayoutProps) {
    const [collapsed, setCollapsed] = useState(false)
    const [location] = useLocation()

    return (
        <>
            <div className="flex h-screen bg-background overflow-hidden">
                {/* Sidebar */}
                <aside
                    className={cn(
                    'flex flex-col h-full border-r border-border transition-all duration-300 ease-in-out shrink-0',
                    'bg-sidebar',
                    collapsed ? 'w-16' : 'w-60'
                    )}
                >

                    {/* Logo */}
                    <div className={cn('flex items-center h-14 px-4 border-b border-border shrink-0', collapsed && 'justify-center px-0')}>
                    {collapsed ? (
                        <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center">
                        <Shield className="w-4 h-4 text-primary" />
                        </div>
                    ) : (
                        <div className="flex items-center gap-2.5">
                        <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center shrink-0">
                            <Shield className="w-4 h-4 text-primary" />
                        </div>
                        <div className="min-w-0">
                            <p className="text-sm font-semibold text-foreground leading-tight truncate" style={{ fontFamily: 'var(--font-display)' }}>ETF Compliance</p>
                            <p className="text-[10px] text-muted-foreground leading-tight">Filing System</p>
                        </div>
                        </div>
                    )}
                    </div>
                
                    {/* NAvigation */}
                    <nav className="flex-1 overflow-y-auto py-3 px-2 space-y-0.5">
                        { 
                            NavItems.map( (item) => {
                                const Icon = item.icon;
                                const isActive = location === item.href;
                                return (
                                    <Link key={item.href} href={item.href}>
                                        <div
                                            className={cn(
                                                'relative flex items-center gap-2.5 px-3 py-2 rounded-lg transition-all duration-150 group',
                                                isActive
                                                ? 'bg-primary/15 text-primary'
                                                : 'text-muted-foreground hover:bg-accent hover:text-foreground'
                                            )}
                                            >
                                            {isActive && (
                                                <span className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 h-5 bg-primary rounded-r-full" />
                                            )}
                                            <Icon className="w-4 h-4 shrink-0" />
                                            <span className="text-sm font-medium flex-1 truncate" style={{ fontFamily: 'var(--font-display)' }}>
                                                {item.label}
                                            </span>
                                            {item.badge && (
                                                <Badge variant="destructive" className="h-4.5 min-w-4.5 px-1 text-[10px] leading-none">
                                                {item.badge}
                                                </Badge>
                                            )}
                                        </div>
                                    </Link>
                                )
                            })
                        }
                    </nav>
                    
                    {/** Bottom Section */}
                    <div className="border-t border-border p-2 space-y-0.5">
                        {collapsed ? (
                            <div>
                            </div>
                        ) : (
                            <>
                            <button
                                onClick={() => toast.info('Settings coming soon')}
                                className="flex items-center gap-2.5 px-3 py-2 w-full rounded-lg text-muted-foreground hover:bg-accent hover:text-foreground transition-colors"
                            >
                                <div className="w-4 h-4 flex-shrink-0" />
                                <span className="text-sm font-medium" style={{ fontFamily: 'var(--font-display)' }}>Settings</span>
                            </button>
                            <button
                                onClick={() => toast.info('Database explorer coming soon')}
                                className="flex items-center gap-2.5 px-3 py-2 w-full rounded-lg text-muted-foreground hover:bg-accent hover:text-foreground transition-colors"
                            >
                                <div className="w-4 h-4 flex-shrink-0" />
                                <span className="text-sm font-medium" style={{ fontFamily: 'var(--font-display)' }}>Database</span>
                            </button>
                            </>
                        )}
                    </div>
                </aside>

                {/** Main Content */}
                <div className="flex flex-col flex-1 min-w-0 overflow-hidden ">
                    {/** Top Bar */}
                    <header className="flex items-center h-14 px-6 border-b border-border bg-background/95 backdrop-blur-sm shrink-0 gap-4">
                        <div className="flex-1 flex items-center gap-3 min-w-0">
                            <div className="min-w-0">
                                <h1 className="text-base font-semibold text-foreground truncate" style={{ fontFamily: 'var(--font-display)' }}>
                                {title}
                                </h1>
                                {subtitle && <p className="text-xs text-muted-foreground truncate">{subtitle}</p>}
                            </div>
                        </div>
                        
                        {/** User Info */}
                        <div className="flex">
                            <button
                                onClick={() => toast.info('Profile coming soon')}
                                className="flex items-center gap-2 rounded-lg px-2 py-1 hover:bg-accent transition-colors"
                            >
                                <div className="w-7 h-7 rounded-full bg-primary/20 flex items-center justify-center">
                                <div className="w-3.5 h-3.5 text-primary" />
                                </div>
                                <span className="text-xs font-medium text-foreground hidden md:block" style={{ fontFamily: 'var(--font-display)' }}>
                                Osama
                                </span>
                            </button>
                        </div>
                    </header>

                    <main className="flex-l overflow-y-auto">
                        {children}
                    </main>
                </div>
            </div>
        
        </>
    )
}

export default DashboardLayout;